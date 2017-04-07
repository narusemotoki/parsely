import abc
import enum
from typing import Optional


RetryMethod = enum.Enum('RetryMethod', ['countdown'])  # type: ignore


class RetryPolicy(metaclass=abc.ABCMeta):
    @abc.abstractproperty
    def max_retries(self) -> Optional[int]:
        """If it returns None, use the default value.
        """
        ...

    @abc.abstractproperty  # type: ignore
    def retry_method(self) -> RetryMethod:
        ...

    @abc.abstractmethod
    def countdown(self, retry_count: int, error: Exception) -> int:
        ...


class CountdownPolicy(RetryPolicy):
    ...


class FibonacciWait(CountdownPolicy):
    """Wait for 1 second, 2 seconds, 3 seconds, 5 seconds ...
    """

    def __init__(self, max_retries: int) -> None:
        self._max_retries = max_retries

    @property
    def max_retries(self) -> Optional[int]:
        return self._max_retries

    @property  # type: ignore
    def retry_method(self) -> RetryMethod:
        return RetryMethod.countdown  # type: ignore

    def countdown(self, retry_count: int, error: Exception) -> int:
        x, y = 1, 1
        for _ in range(retry_count):
            x, y = y, x + y
        return y
