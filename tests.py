import json
import logging
import os
import pkg_resources
import sqlite3
import unittest.mock

import celery
import falcon
import pytest

import parsely
import parsely.database
import parsely.retry

# We don't need actual celery for testing.
celery.Celery = unittest.mock.MagicMock()
parsely.database.db.dbname = 'test.db'

# Disable logger
parsely.logger.setLevel(logging.CRITICAL)


def task_for_test(text: str, number: int):
    pass


class TestParsely:
    def setup_method(self, method):
        self.parsely = parsely.Parsely('test_queue', 'test_broker')

    def teardown_method(self, method):
        parsely._tasks.clear()

    def test_task(self):
        self.parsely.task()(task_for_test)

        (processor, validations), preprocessors = self.parsely._tasks['task_for_test']
        assert len(preprocessors) == 0

        for validation, expect in zip(
                sorted(validations, key=lambda x: x[0]),
                [('number', int), ('text', str)]
        ):
            assert validation == expect

    def test_register_same_task(self):
        self.parsely.task()(task_for_test)

        with pytest.raises(parsely.ParselyError):
            self.parsely.task()(task_for_test)

    def test_queue_name_startw_with__(self):
        with pytest.raises(parsely.ParselyError):
            parsely.Parsely('_queue', 'test_broker')

    def test_retry(self):
        def task_for_retry():
            raise Exception

        with unittest.mock.patch.object(self.parsely, 'celery') as mock_celery:
            def mock_task(handle, bind):
                self.handle = handle

            mock_celery.task.side_effect = mock_task
            self.parsely.task(retry_policy=parsely.retry.FibonacciWait(1))(task_for_retry)
            mock_celery_task = unittest.mock.MagicMock()
            self.handle(mock_celery_task)
            assert mock_celery_task.retry.called

    def test_no_retry(self):
        def task_for_retry():
            raise Exception

        with unittest.mock.patch.object(self.parsely, 'celery') as mock_celery:
            def mock_task(handle, bind):
                self.handle = handle

            mock_celery.task.side_effect = mock_task
            self.parsely.task()(task_for_retry)
            mock_celery_task = unittest.mock.MagicMock()

            with pytest.raises(Exception):
                self.handle(mock_celery_task)
            assert not mock_celery_task.retry.called


class TestProducer:
    def setup_method(self, method):
        self.parsely = parsely.Parsely('test_queue', 'test_broker')
        self.parsely.task()(task_for_test)
        self.producer = parsely.Producer(parsely.HTMLRendler(), True)
        self.mock_req = unittest.mock.MagicMock()
        self.mock_resp = unittest.mock.MagicMock()
        parsely.database.Migrator(parsely.__version__).migrate()
        parsely.database.db.reconnect()

    def teardown_method(self, method):
        parsely._tasks.clear()
        parsely.database.db.get().close()
        os.remove('test.db')

    def test_undefined_queue(self):
        with pytest.raises(falcon.HTTPBadRequest) as e:
            self.producer.on_post(
                self.mock_req, self.mock_resp, 'undefined_queue', 'undefined_task')

        assert e.value.title == "Undefined queue"

    def test_undefined_task(self):
        with pytest.raises(falcon.HTTPBadRequest) as e:
            self.producer.on_post(self.mock_req, self.mock_resp, 'test_queue', 'undefined_task')

        assert e.value.title == "Undefined task"

    def test_empty_payload(self):
        self.mock_req.stream.read.return_value = b""
        with pytest.raises(falcon.HTTPBadRequest) as e:
            self.producer.on_post(self.mock_req, self.mock_resp, 'test_queue', 'task_for_test')

        assert e.value.title == "Empty payload"

    def test_non_json_payload(self):
        self.mock_req.stream.read.return_value = b"This is not a JSON"
        with pytest.raises(falcon.HTTPBadRequest) as e:
            self.producer.on_post(self.mock_req, self.mock_resp, 'test_queue', 'task_for_test')

        assert e.value.title == "Payload is not a JSON"

    def test_lack_message(self):
        self.mock_req.stream.read.return_value = b"{}"
        with pytest.raises(falcon.HTTPBadRequest) as e:
            self.producer.on_post(self.mock_req, self.mock_resp, 'test_queue', 'task_for_test')

        assert e.value.title == "Invalid JSON"

    def test_preprocessor_lack_requirements(self):
        def preprocessor_for_preprocessor_test(text: str):
            pass

        @self.parsely.task(preprocessor_for_preprocessor_test)
        def task_for_preprocessor_test():
            pass

        self.mock_req.stream.read.return_value = json.dumps({
            'message': {}
        }).encode()
        with pytest.raises(falcon.HTTPBadRequest) as e:
            self.producer.on_post(
                self.mock_req, self.mock_resp, 'test_queue', 'task_for_preprocessor_test')

        assert e.value.title == "Missing required filed"

    def test_preprocessor_invalid_type(self):
        def preprocessor_for_preprocessor_test(text: str):
            pass

        @self.parsely.task(preprocessor_for_preprocessor_test)
        def task_for_preprocessor_test():
            pass

        self.mock_req.stream.read.return_value = json.dumps({
            'message': {
                'text': 1
            }
        }).encode()
        with pytest.raises(falcon.HTTPBadRequest) as e:
            self.producer.on_post(
                self.mock_req, self.mock_resp, 'test_queue', 'task_for_preprocessor_test')

        assert e.value.title == "Invalid type"

    def test_task(self):
        def preprocessor_for_preprocessor_test(number: int):
            return {
                'text': str(number)
            }

        @self.parsely.task(preprocessor_for_preprocessor_test)
        def task_for_preprocessor_test(text: str):
            pass

        self.mock_req.stream.read.return_value = json.dumps({
            'message': {
                'number': 1
            }
        }).encode()

        self.producer.on_post(
            self.mock_req, self.mock_resp, 'test_queue', 'task_for_preprocessor_test')

        assert self.parsely._tasks['task_for_preprocessor_test'][0][0].apply_async.called

    def test_on_get(self):
        self.producer.on_get(self.mock_req, self.mock_resp, 'test_queue', 'task_for_test')
        self.mock_resp.content_type = 'text/html'


class TestStaticResource:
    @unittest.mock.patch.object(pkg_resources, "resource_filename")
    def test_on_get_for_installed(self, mock_resource_filename):
        resourcename = "parsely.js"
        mock_resource_filename.return_value = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "parsely", "resources", resourcename
        )

        static_resource = parsely.StaticResource()
        static_resource.is_packaged = True

        mock_resp = unittest.mock.MagicMock()
        static_resource.on_get(unittest.mock.MagicMock(), mock_resp, resourcename)
        mock_resp.content_type = "application/javascript"

    def test_on_get_for_not_installed(self):
        static_resource = parsely.StaticResource()
        static_resource.is_packaged = False

        mock_resp = unittest.mock.MagicMock()
        static_resource.on_get(unittest.mock.MagicMock(), mock_resp, "parsely.js")
        mock_resp.content_type = "application/javascript"


class TestMigrator:
    def teardown_method(self, method):
        parsely._tasks.clear()
        parsely.database.db.get().close()
        os.remove('test.db')

    def test__raise_for_invalid_version(self):
        parsely.database.Migrator(parsely.__version__).migrate()
        with pytest.raises(parsely.ParselyError):
            parsely.database.Migrator('0').migrate()

    def test__run_migration_sql_file(self):
        migrator = parsely.database.Migrator(parsely.__version__)
        migrator._iter_diff = lambda x: [os.path.join("test_resources", "invalid.sql")]

        with pytest.raises(parsely.ParselyError):
            migrator.migrate()


class TestDBManager:
    def setup_method(self, method):
        self.mock_connection_manager = unittest.mock.MagicMock()
        self.db_manager = parsely.DBManager(self.mock_connection_manager)

    def test_process_resource(self):
        self.db_manager.process_resource(
            unittest.mock.MagicMock(), unittest.mock.MagicMock(), unittest.mock.MagicMock(),
            unittest.mock.MagicMock()
        )
        assert self.mock_connection_manager.reconnect.called

    def test_process_response_without_connection(self):
        self.mock_connection_manager.get = unittest.mock.MagicMock(return_value=None)
        self.db_manager.process_response(
            unittest.mock.MagicMock(), unittest.mock.MagicMock(), unittest.mock.MagicMock(), True
        )

        assert not self.mock_connection_manager.commit.called
        assert not self.mock_connection_manager.rollback.called

    def test_process_response_succeeded(self):
        self.db_manager.process_response(
            unittest.mock.MagicMock(), unittest.mock.MagicMock(), unittest.mock.MagicMock(), True
        )
        assert self.mock_connection_manager.get.return_value.commit.called
        assert not self.mock_connection_manager.get.return_value.rollback.called

    def test_process_response_failed(self):
        self.db_manager.process_response(
            unittest.mock.MagicMock(), unittest.mock.MagicMock(), unittest.mock.MagicMock(), False
        )
        assert not self.mock_connection_manager.get.return_value.commit.called
        assert self.mock_connection_manager.get.return_value.rollback.called

    def test_process_response_failed_with_closed_connection(self):
        self.mock_connection_manager.get.return_value.rollback = unittest.mock.MagicMock(
            side_effect=sqlite3.ProgrammingError)
        self.db_manager.process_response(
            unittest.mock.MagicMock(), unittest.mock.MagicMock(), unittest.mock.MagicMock(), False
        )


class TestListTaskResource:
    def setup_method(self, method):
        self.resource = parsely.TaskListResource(parsely.HTMLRendler())

    def teardown_method(self, method):
        parsely._tasks.clear()

    def test_on_get(self):
        queue_name = 'test_queue'
        parsely.Parsely(queue_name, 'test_broker')
        mock_resp = unittest.mock.MagicMock()
        self.resource.on_get(unittest.mock.MagicMock(), mock_resp, queue_name)

        assert mock_resp.content_type == 'text/html'

    def test_on_get_unknown_queue(self):
        mock_resp = unittest.mock.MagicMock()
        with pytest.raises(falcon.HTTPNotFound):
            self.resource.on_get(unittest.mock.MagicMock(), mock_resp, "unknow_queue")

    def test__list_task_name(self):
        queue_name = 'test_queue'
        b = parsely.Parsely(queue_name, 'test_broker')
        assert len(list(self.resource._list_task_name(queue_name))) == 0

        b.task()(task_for_test)
        assert len(list(self.resource._list_task_name(queue_name))) == 1


class TestQueueListResource:
    def setup_method(self, method):
        self.rendler = unittest.mock.MagicMock()
        self.resource = parsely.QueueListResource(self.rendler)

    def teardown_method(self, method):
        parsely._tasks.clear()

    def test_on_get(self):
        queue_names = ["first_quque", "second_queue"]
        for queue_name in queue_names:
            parsely.Parsely(queue_name, 'test_broker')

        self.resource.on_get(unittest.mock.MagicMock(), unittest.mock.MagicMock())
        self.rendler.render.assert_called_once_with("queue_list.html", queue_names=queue_names)


def test_producer():
    assert isinstance(parsely.producer(), falcon.api.API)


def test_fibonacci_wait():
    retry_count = 10
    fibonacci_wait = parsely.retry.FibonacciWait(retry_count)
    assert fibonacci_wait.max_retries == retry_count
    assert fibonacci_wait.retry_method == parsely.retry.RetryMethod.countdown

    for i, expect in enumerate([1, 2, 3, 5, 8, 13, 21, 34, 55, 89]):
        assert fibonacci_wait.countdown(i, None) == expect
