import parsely

p = parsely.Parsely('example', 'redis://localhost:6379/0')
celery = p.celery


def two_times(text: str) -> dict:
    return {
        'text': text * 2
    }


@p.task(two_times)
def echo(text: str) -> None:
    print(text)
