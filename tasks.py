import brokkoly

b = brokkoly.Brokkoly('example', 'redis://localhost:6379/0')
celery = b.celery


def two_times(text: str) -> dict:
    return {
        'text': text * 2
    }


@b.task(two_times)
def echo(text: str) -> None:
    print(text)
