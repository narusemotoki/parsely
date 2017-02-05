import collections
import inspect
import json
import os
import pkg_resources
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


__all__ = ['BrokkolyError', 'Brokkoly', 'producer']
__author__ = "Motoki Naruse"
__copyright__ = "Motoki Naruse"
__credits__ = ["Motoki Naruse"]
__email__ = "motoki@naru.se"
__license__ = "MIT"
__maintainer__ = "Motoki Naruse"
__version__ = "0.1.0"


Validation = List[Tuple[str, Any]]
Processor = collections.namedtuple('Processor', ['func', 'validation'])
Message = Dict[str, Any]

_tasks = collections.defaultdict(dict)  # type: collections.defaultdict


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


class Producer:
    def __init__(self) -> None:
        self._jinja2 = jinja2.Environment(loader=jinja2.ChoiceLoader([
            jinja2.PackageLoader(__name__, 'resources'),
            jinja2.FileSystemLoader('resources'),
        ]))

    def _render(self, template: str, **kwargs) -> str:
        return self._jinja2.get_template(template).render(**kwargs)

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
        resp.status = falcon.HTTP_202
        resp.body = "{}"

    def on_get(
            self, req: falcon.request.Request, resp: falcon.response.Response, queue_name: str,
            task_name: str
    ) -> None:
        self._validate_queue_and_task(queue_name, task_name)

        resp.content_type = 'text/html'
        resp.body = self._render("enqueue.html", queue_name=queue_name, task_name=task_name)


class StaticResource:
    def __init__(self) -> None:
        self._packagepath = os.path.dirname(os.path.abspath(__file__))
        self._distributionpath = os.path.dirname(self._packagepath)

        for info in pkg_resources.WorkingSet():  # type: ignore
            if info.project_name == __name__ and info.location != self._distributionpath:
                self.is_packaged = True
                break
        else:
            self.is_packaged = False

    def _resource_filename(self, filename: str) -> str:
        if self.is_packaged:
            return pkg_resources.resource_filename(__name__, "resources/{}".format(filename))
        return os.path.join(self._packagepath, 'resources', filename)

    def _read_resource(self, filename: str) -> bytes:
        with open(self._resource_filename(filename), 'rb') as f:
            return f.read()

    def on_get(
            self, req: falcon.request.Request, resp: falcon.response.Response, filename: str
    ) -> None:
        resp.content_type = {
            '.css': "text/css",
            '.js': "application/javascript",
        }[os.path.splitext(filename)[1]]

        resp.body = self._read_resource(filename)


def producer(path: Optional[str]=None) -> falcon.api.API:
    application = falcon.API()
    for controller, route in [
            (StaticResource(), "/__static__/{filename}"),
            (Producer(), "/{queue_name}/{task_name}"),
    ]:
        application.add_route("/{}{}".format(path, route) if path else route, controller)

    return application
