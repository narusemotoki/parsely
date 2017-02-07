import logging

import brokkoly

import tasks  # NOQA

application = brokkoly.producer(log_level=logging.DEBUG)
