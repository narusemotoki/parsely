import collections
import inspect
import json
import logging
import os
import sqlite3
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
)

import celery
import falcon
import falcon.request
import falcon.response
import jinja2

import brokkoly.database
import brokkoly.resource


__all__ = ['BrokkolyError', 'Brokkoly', 'producer']
__author__ = "Motoki Naruse"
__copyright__ = "Motoki Naruse"
__credits__ = ["Motoki Naruse"]
__email__ = "motoki@naru.se"
__license__ = "MIT"
__maintainer__ = "Motoki Naruse"
__version__ = "0.3.0"


Validation = List[Tuple[str, Any]]
Processor = collections.namedtuple('Processor', ['func', 'validation'])
Message = Dict[str, Any]

_tasks = collections.defaultdict(dict)  # type: collections.defaultdict


logger = logging.getLogger(__name__)


class BrokkolyError(Exception):
    pass


class Brokkoly:
    def __init__(self, name: str, broker: str) -> None:
        if name.startswith('_'):
            # Because the names is reserved for control.
            raise BrokkolyError("Queue name starting with _ is not allowed.")
        self.celery = celery.Celery(name, broker=broker)
        self._tasks = _tasks[name]

    def task(self, *preprocessors: Callable) -> Callable:
        def wrapper(f: Callable):
            if f.__name__ in self._tasks:
                raise BrokkolyError("{} is already registered".format(f.__name__))

            self._tasks[f.__name__] = (
                Processor(self.celery.task(f), _prepare_validation(f)),
                [
                    Processor(
                        preprocessor,
                        _prepare_validation(preprocessor)
                    )
                    for preprocessor in preprocessors
                ]
            )
            return f
        return wrapper


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
    def __init__(self, rendler: HTMLRendler) -> None:
        self._rendler = rendler

    def _recurse(self, message: Message, preprocessors: List[Processor]) -> Message:
        if preprocessors:
            (preprocess, preprocess_validation), *tail = preprocessors
            return self._recurse(preprocess(**_validate(message, preprocess_validation)), tail)
        return message

    def _validate_queue_and_task(
            self, queue_name: str, task_name: str) -> Tuple[Processor, List[Processor]]:
        # _tasks is defaultdict, it deoesn't raise KeyError.
        if queue_name not in _tasks:
            raise falcon.HTTPBadRequest(
                "Undefined queue", "{} is undefined queue".format(queue_name))
        queue_tasks = _tasks[queue_name]

        try:
            return queue_tasks[task_name]
        except KeyError:
            raise falcon.HTTPBadRequest("Undefined task", "{} is undefined task".format(task_name))

    def on_post(
            self, req: falcon.request.Request, resp: falcon.response.Response, queue_name: str,
            task_name: str
    ) -> None:
        (task, validation), preprocessors = self._validate_queue_and_task(queue_name, task_name)
        payload = req.stream.read().decode()
        if not payload:
            raise falcon.HTTPBadRequest(
                "Empty payload",
                "Even your task doesn't need any arguments, payload must have message filed"
            )

        try:
            message = json.loads(payload)['message']
        except ValueError:  # Python 3.4 doesn't have json.JSONDecodeError
            raise falcon.HTTPBadRequest("Payload is not a JSON", "The payload must be a JSON")
        except KeyError:
            raise falcon.HTTPBadRequest("Invalid JSON", "JSON must have message field")

        task.apply_async(
            kwargs=_validate(self._recurse(message, preprocessors), validation),
            serializer='json',
            compression='zlib'
        )
        brokkoly.database.MessageLog.create(queue_name, task_name, json.dumps(message))
        brokkoly.database.MessageLog.eliminate(queue_name, task_name)
        resp.status = falcon.HTTP_202
        resp.body = "{}"

    def on_get(
            self, req: falcon.request.Request, resp: falcon.response.Response, queue_name: str,
            task_name: str
    ) -> None:
        self._validate_queue_and_task(queue_name, task_name)
        messages = brokkoly.database.MessageLog.list_by_queue_name_and_task_name(
            queue_name, task_name)

        resp.content_type = 'text/html'
        resp.body = self._rendler.render(
            "enqueue.html", queue_name=queue_name, task_name=task_name, messages=messages)


class TaskListResource:
    def __init__(self, rendler: HTMLRendler) -> None:
        self._rendler = rendler

    def on_get(
            self, req: falcon.request.Request, resp: falcon.response.Response, queue_name: str
    ) -> None:
        try:
            task_names = self._list_task_name(queue_name)
        except KeyError:
            raise falcon.HTTPNotFound(
                title="Undefined queue",
                description="{} is undefined queue".format(queue_name)
            )

        resp.content_type = 'text/html'
        resp.body = self._rendler.render(
            "task_list.html", queue_name=queue_name, task_names=task_names)

    def _list_task_name(self, queue_name: str) -> Iterable[str]:
        # Check with "in" because _tasks is defaultdict.
        if queue_name in _tasks:
            return _tasks[queue_name].keys()
        raise KeyError


class QueueListResource:
    def __init__(self, rendler: HTMLRendler) -> None:
        self._rendler = rendler

    def on_get(self, req: falcon.request.Request, resp: falcon.response.Response) -> None:
        resp.content_type = 'text/html'
        resp.body = self._rendler.render("queue_list.html", queue_names=self._list_queue_name())

    def _list_queue_name(self) -> List[str]:
        return sorted(_tasks.keys())


class StaticResource:
    def _read_resource(self, filename: str) -> bytes:
        with open(brokkoly.resource.resource_filename(filename), 'rb') as f:
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
            self, connection_manager: brokkoly.database.ThreadLocalDBConnectionManager) -> None:
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


def init_logger(log_level: int) -> None:
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)-5.5s [%(name)s:%(lineno)s] %(message)s"
    )


def producer(*, path: Optional[str]=None, log_level=logging.ERROR) -> falcon.api.API:
    init_logger(log_level)
    brokkoly.database.db.dbname = "brokkoly.db"

    brokkoly.database.Migrator(__version__).migrate()

    application = falcon.API(middleware=[DBManager(brokkoly.database.db)])
    rendler = HTMLRendler()
    for controller, route in [
            (StaticResource(), "/__static__/{filename}"),
            (Producer(rendler), "/{queue_name}/{task_name}"),
            (QueueListResource(rendler), "/"),
            (TaskListResource(rendler), "/{queue_name}"),
    ]:
        application.add_route("/{}{}".format(path, route) if path else route, controller)

    return application
