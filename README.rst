=======
Parsely
=======

.. image:: https://travis-ci.org/narusemotoki/parsely.svg?branch=master
    :target: https://travis-ci.org/narusemotoki/parsely

Parsely is a framework for enqueuing messages via HTTP request for celery.

`CHANGELOG <https://github.com/narusemotoki/parsely/blob/master/CHANGELOG.rst>`_

Example
=======

tasks.py:
---------

.. code-block:: python

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

:code:`two_times` works as pre processor. It works before enqueing. It means it can return BadRequest to your client. Parsely validate message with typehint. Also you can have extra validation and any other process here.

Run :code:`celery -A tasks worker --loglevel=info`

producer.py:
------------

.. code-block:: python

   import parsely

   import tasks  # NOQA

   application = parsely.producer()

`producer` is WSGI application. You need to import your `tasks` for put message into queue.

Run with uWSGI :code:`uwsgi --http :8080 --wsgi-file producer.py --enable-threads --thunder-lock --master`

Send Test Message!
------------------

:code:`curl -X POST -d '{"message":{"text": "Hello"}}' http://localhost:8080/example/echo`

1. `producer` receives your request
2. `producer` validates your message having `text` and the type is `str` or not. `text`(str)`  is from typehint of :code:`two_times`
3. `producer` validates `two_times` returned value having `text` and the type is `str` or not. `text`(str)`  is from typehint of :code:`echo`
4. `producer` put message :code:`{"message":{"text":"HelloHello"}}` into queue.
5. `curl` receives response.
6. `Celery` calls :code:`echo`


Extra
=====

Preprocessor
------------

preprocessor is optional. if you don't need it, you can:

.. code-block:: python

   @p.task()
   def echo(text: str) -> None:
       print(text)

Also you can give multiple preprocessor:

.. code-block:: python

   @p.task(two_times, two_times)
   def echo(text: str) -> None:
       print(text)


logging
-------

Parsely uses logger witch named "parsely", so you can set log level like:

.. code-block:: python

   import logging


   parsely_logger = logging.getLogger('parsely')
   parsely_logger.setLevel(logging.DEBUG)
