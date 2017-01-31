import json
import inspect
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Tuple,
)

import celery
import celery.app.task  # NOQA
import falcon
import falcon.request
import falcon.response


_celery = celery.Celery('tasks', broker='redis://localhost:6379/0')
_Validation = List[Tuple[str, Any]]
_tasks = {}  # type: Dict[celery.app.task.Task, _Validation]


def _prepare_validation(f: Callable) -> _Validation:
    fullspec = inspect.getfullargspec(f)

    args = {}
    for arg_name in fullspec.args:
        args[arg_name] = Any

    for arg_name, arg_type in fullspec.annotations.items():
        # returning type is doesn't matter. This is for input checking.
        if arg_name != 'return':
            args[arg_name] = arg_type

    return list(args.items())


def register(f: Callable) -> Callable:
    _tasks[f.__name__] = (_celery.task(f), _prepare_validation(f))
    return f


def _validate(message: Dict[str, Any], validation: _Validation) -> Dict[str, Any]:
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


class Enqueue:
    def on_post(self, req: falcon.request.Request,
                resp: falcon.response.Response, queue_name: str) -> None:
        try:
            task, validation = _tasks[queue_name]
        except KeyError:
            raise falcon.HTTPBadRequest(
                "Unknown queue name", "{} is unknown queue name".format(queue_name))

        try:
            message = json.loads(req.stream.read().decode())['message']
        except KeyError:
            raise falcon.HTTPBadRequest("Invalid JSON", "JSON must have message field")

        task.apply_async(
            kwargs=_validate(message, validation), serializer='json', compression='zlib')


@register
def async_order(order_id: int) -> None:
    pass
