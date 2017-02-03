import collections
import json
import inspect
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


__version__ = '0.0.1'


Validation = List[Tuple[str, Any]]
Processor = collections.namedtuple('Processor', ['func', 'validation'])

_tasks = collections.defaultdict(dict)  # type: collections.defaultdict


class BrokkolyError(Exception):
    pass


class Brokkoly:
    def __init__(self, name: str, broker: str) -> None:
        self.celery = celery.Celery('tasks', broker=broker)
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

    args = {}
    for arg_name in fullspec.args:
        args[arg_name] = Any

    for arg_name, arg_type in fullspec.annotations.items():
        # returning type is doesn't matter. This is for input checking.
        if arg_name != 'return':
            args[arg_name] = arg_type

    return list(args.items())


def _validate(message: Dict[str, Any], validation: Validation) -> Dict[str, Any]:
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
    def on_post(
            self, req: falcon.request.Request, resp: falcon.response.Response, queue_name: str,
            task_name: str
    ) -> None:
        # _tasks is defaultdict, it deoesn't raise KeyError.
        if queue_name not in _tasks:
            raise falcon.HTTPBadRequest(
                "Undefined queue", "{} is undefined queue".format(queue_name))
        queue_tasks = _tasks[queue_name]

        try:
            (task, validation), preprocessors = queue_tasks[task_name]
        except KeyError:
            raise falcon.HTTPBadRequest("Undefined task", "{} is undefined task".format(task_name))

        payload = req.stream.read().decode()
        if not payload:
            raise falcon.HTTPBadRequest(
                "Empty payload",
                "Even your task doesn't need any arguments, payload must have message filed"
            )

        try:
            message = json.loads(payload)['message']
        except ValueError:  # Python 3.4 doesn't have json.JSONDecodeError
            raise falcon.HTTPBadRequest("Payload is not a JSON", "The payload must be JSON")
        except KeyError:
            raise falcon.HTTPBadRequest("Invalid JSON", "JSON must have message field")

        for preprocess, preprocess_validation in preprocessors:
            message = preprocess(**_validate(message, preprocess_validation))

        task.apply_async(
            kwargs=_validate(message, validation), serializer='json', compression='zlib')


def producer(path_prefix: Optional[str] = None) -> falcon.api.API:
    application = falcon.API()
    defaule_route = "/{queue_name}/{task_name}"
    route = '/{}{}'.format(path_prefix, defaule_route) if path_prefix else defaule_route
    application.add_route(route, Producer())

    return application
