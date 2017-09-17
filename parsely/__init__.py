import collections
import inspect
import json
import logging
import os
import sqlite3
import types
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
)

import celery
import falcon
import falcon.request
import falcon.response
import jinja2

import parsely.retry
import parsely.database
import parsely.resource


__all__ = ['ParselyError', 'Parsely']
__author__ = "Motoki Naruse"
__copyright__ = "Motoki Naruse"
__credits__ = ["Motoki Naruse"]
__email__ = "motoki@naru.se"
__license__ = "MIT"
__maintainer__ = "Motoki Naruse"
__version__ = "0.7.0"


Validation = List[Tuple[str, Any]]
Processor = collections.namedtuple('Processor', ['func', 'validation'])
Tasks = Dict[str, Tuple[Processor, List[Processor]]]
Message = Dict[str, Any]
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
_logger_handler = logging.StreamHandler()
_logger_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(_logger_handler)


class ParselyError(Exception):
    pass


def copy_function(function: Callable, name: str) -> Callable:
    return types.FunctionType(  # type: ignore
        function.__code__,  # type: ignore
        function.__globals__,  # type: ignore
        name,
        function.__defaults__,  # type: ignore
        function.__closure__  # type: ignore
    )


class Parsely:
    def __init__(self, name: str, broker: str) -> None:
        if name.startswith('_'):
            # Because the names is reserved for control.
            raise ParselyError("Queue name starting with _ is not allowed.")
        self.celery = celery.Celery(name, broker=broker)
        self._tasks = {}  # type: Tasks
        self._queue_name = name

    def task(
            self, *preprocessors: Callable, retry_policy: Optional[parsely.retry.RetryPolicy]=None
    ) -> Callable:
        """Return a function for register a function as Celery task.

        :param preprocessors: returning value of a preprocessor will be passed to the next
        preprocessor, then all preprocessors are finished, the last result will be passed to
        function f.
        :param retry_policy: If it is not None, when an exception is raised by function f, it will
        be retried based on this policy.
        """
        def wrapper(f: Callable) -> Callable:
            """Register the function as Celery task.
            """
            if f.__name__ in self._tasks:
                raise ParselyError("{} is already registered.".format(f.__name__))

            def handle(celery_task, *args, **kwargs) -> None:
                try:
                    return f(*args, **kwargs)
                except Exception as e:
                    if not retry_policy:
                        raise e
                    error = e
                celery_task.retry(
                    countdown=retry_policy.countdown(celery_task.request.retries, error),
                    max_retries=retry_policy.max_retries,
                    exc=error
                )

            # Copy handle and give a name because Celery uses the function name. If it is
            # duplicated, can't control which handler will be called.
            specialized_handle = copy_function(handle, f.__name__)
            self._tasks[f.__name__] = (
                Processor(self.celery.task(specialized_handle, bind=True), _prepare_validation(f)),
                [
                    Processor(preprocessor, _prepare_validation(preprocessor))
                    for preprocessor in preprocessors
                ]
            )
            logger.info("Task %s is registered queue %s", f.__name__, self._queue_name)
            return f
        return wrapper

    def make_producer(
            self, *,
            path: Optional[str]=None,
            enable_database=True,
            dbname: str="parsely.db"
    ) -> falcon.api.API:
        """Create WSGI application for enqueing.

        :param path: You can give extra path. If it's None, an entry point is
        "/{queue_name}/{task_name}". If it isn't None, an entry point is
        "/{path}/{queue_name}/{task_name}"
        :param enable_database: You can switch Parsely records messages into database or not.
        :return: WSGI compatible object.
        """
        parsely.database.db.dbname = dbname

        if enable_database:
            parsely.database.Migrator(__version__).migrate()
            middlewares = [DBManager(parsely.database.db)]
        else:
            middlewares = []

        application = falcon.API(middleware=middlewares)
        rendler = HTMLRendler()
        for controller, route in [
                (StaticResource(), "/__static__/{filename}"),
                (
                    Producer(rendler, enable_database, self._queue_name, self._tasks),
                    "/{}/{{task_name}}".format(self._queue_name)
                ),
                (
                    TaskListResource(rendler, self._queue_name, self._tasks),
                    "/{}".format(self._queue_name)
                ),
        ]:
            application.add_route("/{}{}".format(path, route) if path else route, controller)

        return application


def _prepare_validation(f: Callable) -> Validation:
    fullspec = inspect.getfullargspec(f)

    args = {arg_name: Any for arg_name in fullspec.args}
    for arg_name, arg_type in fullspec.annotations.items():
        # returning type is doesn't matter. This is for input checking.
        if arg_name != 'return':
            args[arg_name] = arg_type

    return list(args.items())


def _validate(message: Message, validation: Validation) -> Message:
    validated = {}
    for arg_name, arg_type in validation:
        try:
            value = message[arg_name]
        except KeyError:
            raise falcon.HTTPBadRequest(
                "Missing required filed", "{} is required".format(arg_name))

        if arg_type != Any and not isinstance(value, arg_type):
            raise falcon.HTTPBadRequest(
                "Invalid type", "{} must be {} type".format(arg_name, arg_type.__name__))

        validated[arg_name] = value

    return validated


class HTMLRendler:
    def __init__(self) -> None:
        self._jinja2 = jinja2.Environment(loader=jinja2.ChoiceLoader([
            jinja2.PackageLoader(__name__, 'resources'),
            jinja2.FileSystemLoader('resources'),
        ]), extensions=[
            'jinja2_highlight.HighlightExtension'
        ])
        self._jinja2.filters['pretty_print_json'] = lambda source: json.dumps(
            json.loads(source), indent=4, sort_keys=True)

    def render(self, template: str, **kwargs) -> str:
        return self._jinja2.get_template(template).render(**kwargs)


class Producer:
    def __init__(
            self, rendler: HTMLRendler, enable_database: bool, queue_name: str, tasks: Tasks
    ) -> None:
        self._rendler = rendler
        self._enable_database = enable_database
        self._queue_name = queue_name
        self._tasks = tasks

    def _recurse(self, message: Message, preprocessors: List[Processor]) -> Message:
        if preprocessors:
            (preprocess, preprocess_validation), *tail = preprocessors
            return self._recurse(preprocess(**_validate(message, preprocess_validation)), tail)
        return message

    def _validate_payload(self, req: falcon.request.Request) -> Dict[str, Any]:
        payload = req.stream.read().decode()
        if not payload:
            raise falcon.HTTPBadRequest(
                "Empty payload",
                "Even your task doesn't need any arguments, payload must have message filed"
            )
        try:
            return json.loads(payload)
        except ValueError:  # Python 3.4 doesn't have json.JSONDecodeError
            raise falcon.HTTPBadRequest("Payload is not a JSON", "The payload must be a JSON")

    def on_post(
            self, req: falcon.request.Request, resp: falcon.response.Response, task_name: str
    ) -> None:
        try:
            (task, validation), preprocessors = self._tasks[task_name]
        except KeyError:
            raise falcon.HTTPBadRequest("Undefined task", "{} is undefined task".format(task_name))

        payload = self._validate_payload(req)

        logger.info(
            "Received message for Queue %s, Task %s, %s", self._queue_name, task_name, payload)

        try:
            message = payload['message']
        except KeyError:
            raise falcon.HTTPBadRequest("Invalid JSON", "JSON must have message field")

        task.apply_async(
            kwargs=_validate(self._recurse(message, preprocessors), validation),
            serializer='json',
            compression='zlib',
            countdown=payload.get('delay', 0)
        )

        if self._enable_database:
            parsely.database.MessageLog.create(self._queue_name, task_name, json.dumps(message))
            parsely.database.MessageLog.eliminate(self._queue_name, task_name)
        resp.status = falcon.HTTP_202
        resp.body = "{}"

    def on_get(
            self, req: falcon.request.Request, resp: falcon.response.Response, task_name: str
    ) -> None:
        if self._enable_database:
            messages = parsely.database.MessageLog.iter_by_queue_name_and_task_name(
                self._queue_name, task_name)
        else:
            messages = iter([])

        resp.content_type = 'text/html'
        resp.body = self._rendler.render(
            "enqueue.html", queue_name=self._queue_name, task_name=task_name, messages=messages)


class TaskListResource:
    def __init__(self, rendler: HTMLRendler, queue_name: str, tasks: Tasks) -> None:
        self._rendler = rendler
        self._queue_name = queue_name
        self._tasks = tasks

    def on_get(self, req: falcon.request.Request, resp: falcon.response.Response,) -> None:
        task_names = self._tasks.keys()
        resp.content_type = 'text/html'
        resp.body = self._rendler.render(
            "task_list.html", queue_name=self._queue_name, task_names=task_names)


class StaticResource:
    def _read_resource(self, filename: str) -> bytes:
        with open(parsely.resource.resource_filename(filename), 'rb') as f:
            return f.read()

    def on_get(
            self, req: falcon.request.Request, resp: falcon.response.Response, filename: str
    ) -> None:
        resp.content_type = {
            '.css': "text/css",
            '.js': "application/javascript",
        }[os.path.splitext(filename)[1]]

        resp.body = self._read_resource(filename)


class DBManager:
    def __init__(
            self, connection_manager: parsely.database.ThreadLocalDBConnectionManager) -> None:
        self.connection_manager = connection_manager

    def process_resource(
            self, req: falcon.request.Request, resp: falcon.response.Response, resource, params
    ) -> None:
        self.connection_manager.reconnect()

    def process_response(
            self, req: falcon.request.Request, resp: falcon.response.Response, resource,
            req_succeeded: bool
    ) -> None:
        connection = self.connection_manager.get()
        if not connection:
            # make connection only requested URL is matched any route.
            return

        if req_succeeded:
            connection.commit()
            connection.close()
            return

        # I think no way to know the connection is alive or not. When the thread is reused, and
        # it was passed process_resource method, connection is not None and comes here.
        try:
            connection.rollback()
            connection.close()
        except sqlite3.ProgrammingError:
            # With above reason, I think we don't need to report this one as exception.
            logger.debug("Failed to rollback or close SQLite3 connection.")
