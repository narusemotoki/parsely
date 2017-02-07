import json
import os
import pkg_resources
import sqlite3
import unittest.mock

import celery
import falcon
import pytest

import brokkoly
import brokkoly.database

# We don't need actual celery for testing.
celery.Celery = unittest.mock.MagicMock()
brokkoly.database.db.dbname = 'test.db'


def task_for_test(text: str, number: int):
    pass


class TestBrokkoly:
    def setup_method(self, method):
        self.brokkoly = brokkoly.Brokkoly('test_queue', 'test_broker')

    def teardown_method(self, method):
        brokkoly._tasks.clear()

    def test_task(self):
        self.brokkoly.task()(task_for_test)

        (processor, validations), preprocessors = self.brokkoly._tasks['task_for_test']
        assert len(preprocessors) == 0

        for validation, expect in zip(
                sorted(validations, key=lambda x: x[0]),
                [('number', int), ('text', str)]
        ):
            assert validation == expect

    def test_register_same_task(self):
        self.brokkoly.task()(task_for_test)

        with pytest.raises(brokkoly.BrokkolyError):
            self.brokkoly.task()(task_for_test)

    def test_queue_name_startw_with__(self):
        with pytest.raises(brokkoly.BrokkolyError):
            brokkoly.Brokkoly('_queue', 'test_broker')


class TestProducer:
    def setup_method(self, method):
        self.brokkoly = brokkoly.Brokkoly('test_queue', 'test_broker')
        self.brokkoly.task()(task_for_test)
        self.producer = brokkoly.Producer()
        self.mock_req = unittest.mock.MagicMock()
        self.mock_resp = unittest.mock.MagicMock()
        brokkoly.database.Migrator(brokkoly.__version__).migrate()
        brokkoly.database.db.reconnect()

    def teardown_method(self, method):
        brokkoly._tasks.clear()
        brokkoly.database.db.get().close()
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

        @self.brokkoly.task(preprocessor_for_preprocessor_test)
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

        @self.brokkoly.task(preprocessor_for_preprocessor_test)
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

        @self.brokkoly.task(preprocessor_for_preprocessor_test)
        def task_for_preprocessor_test(text: str):
            pass

        self.mock_req.stream.read.return_value = json.dumps({
            'message': {
                'number': 1
            }
        }).encode()

        self.producer.on_post(
            self.mock_req, self.mock_resp, 'test_queue', 'task_for_preprocessor_test')

        assert self.brokkoly._tasks['task_for_preprocessor_test'][0][0].apply_async.called

    def test_on_get(self):
        self.producer.on_get(self.mock_req, self.mock_resp, 'test_queue', 'task_for_test')
        self.mock_resp.content_type = 'text/html'


class TestStaticResource:
    @unittest.mock.patch.object(pkg_resources, "resource_filename")
    def test_on_get_for_installed(self, mock_resource_filename):
        resourcename = "brokkoly.js"
        mock_resource_filename.return_value = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "brokkoly", "resources", resourcename
        )

        static_resource = brokkoly.StaticResource()
        static_resource.is_packaged = True

        mock_resp = unittest.mock.MagicMock()
        static_resource.on_get(unittest.mock.MagicMock(), mock_resp, resourcename)
        mock_resp.content_type = "application/javascript"

    def test_on_get_for_not_installed(self):
        static_resource = brokkoly.StaticResource()
        static_resource.is_packaged = False

        mock_resp = unittest.mock.MagicMock()
        static_resource.on_get(unittest.mock.MagicMock(), mock_resp, "brokkoly.js")
        mock_resp.content_type = "application/javascript"


class TestMigrator:
    def teardown_method(self, method):
        brokkoly._tasks.clear()
        brokkoly.database.db.get().close()
        os.remove('test.db')

    def test__raise_for_invalid_version(self):
        brokkoly.database.Migrator(brokkoly.__version__).migrate()
        with pytest.raises(brokkoly.BrokkolyError):
            brokkoly.database.Migrator('0').migrate()

    def test__run_migration_sql_file(self):
        migrator = brokkoly.database.Migrator(brokkoly.__version__)
        migrator._iter_diff = lambda x: [os.path.join("test_resources", "invalid.sql")]

        with pytest.raises(brokkoly.BrokkolyError):
            migrator.migrate()


class TestDBManager:
    def setup_method(self, method):
        self.mock_connection_manager = unittest.mock.MagicMock()
        self.db_manager = brokkoly.DBManager(self.mock_connection_manager)

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


def test_producer():
    assert isinstance(brokkoly.producer(), falcon.api.API)
