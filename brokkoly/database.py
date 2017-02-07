import contextlib
import datetime
import logging
import os
import sqlite3
import threading
from typing import (
    Iterator,
    Optional,
)

import brokkoly.resource


logger = logging.getLogger(__name__)


class ThreadLocalDBConnectionManager:
    _connections = {}  # type: Dict[int, sqlite3.Connection]
    dbname = None  # type: Optional[str]

    def get(self) -> Optional[sqlite3.Connection]:
        return self._connections.get(threading.get_ident())

    def reconnect(self) -> None:
        id = threading.get_ident()
        connection = sqlite3.connect(self.dbname)
        connection.row_factory = sqlite3.Row
        logger.debug("Connect sqlite3 (%s) for %s", connection, id)
        self._connections[id] = connection


db = ThreadLocalDBConnectionManager()


class Migrator:
    def __init__(self, brokkoly_version: str) -> None:
        self.brokkoly_version = brokkoly_version

    def _has_database(self):
        with contextlib.closing(db.get().cursor()) as cursor:
            cursor.execute("""
            SELECT EXISTS(
                SELECT * FROM sqlite_master
                WHERE
                    sqlite_master.type = 'table' AND
                    sqlite_master.name = 'migrations'
            );
            """)
            return cursor.fetchone()[0]

    def _get_migration_version(self):
        with contextlib.closing(db.get().cursor()) as cursor:
            cursor.execute("""
            SELECT version
            FROM migrations
            ORDER BY version DESC
            LIMIT 1
            """)
            return cursor.fetchone()[0]

    def _iter_diff(self, version: str) -> Iterator[str]:
        """Returning data must be sorted.
        """
        logger.info("resource_dir: %s", brokkoly.resource.resource_dir)
        migration_dir = os.path.join(brokkoly.resource.resource_dir, 'migrations')
        return (
            os.path.join(migration_dir, filename)
            for filename in sorted(os.listdir(migration_dir))
            if os.path.splitext(filename)[0] > version
        )

    def _raise_for_invalid_version(self, schema_version: str) -> None:
        if schema_version > self.brokkoly_version:
            raise brokkoly.BrokkolyError(
                "Blokkory version: {}, Database schema version: {}. The database is setup with  "
                "newer version of Brokkoly. It doesn't provide downgrade database. Please upgrade "
                "Brokkoly.".format(self.brokkoly_version, schema_version)
            )

    def _run_migration_sql_file(self, filename: str) -> None:
        with open(filename, 'r') as f:
            sql = f.read()

        with contextlib.closing(db.get().cursor()) as cursor:
            try:
                cursor.executescript(sql)
            except sqlite3.Error as e:
                logger.exception("Failed to run migration: %s", filename)
                raise brokkoly.BrokkolyError("Failed to run migration") from e

    def _migrate(self) -> None:
        schema_version = self._get_migration_version() if self._has_database() else '0'
        logger.info("schema_version: %s", schema_version)
        self._raise_for_invalid_version(schema_version)

        for sql_file in self._iter_diff(schema_version):
            logger.info("Run migration: %s", sql_file)
            self._run_migration_sql_file(sql_file)

    def migrate(self) -> None:
        db.reconnect()
        try:
            self._migrate()
        finally:
            db.get().close()


class MessageLog:
    def __init__(
            self, *, id: int=None, queue_name: str, task_name: str, message: str,
            created_at: datetime.datetime
    ) -> None:
        self.id = id
        self.queue_name = queue_name
        self.task_name = task_name
        self.message = message
        self.created_at = created_at

    @classmethod
    def get_by_id(cls, id: int) -> Optional['MessageLog']:
        with contextlib.closing(db.get().cursor()) as cursor:
            cursor.execute("SELECT * FROM message_logs WHERE message_logs.id = ?", (id, ))
            return cls.from_sqlite3_row(cursor.fetchone())

    @classmethod
    def create(cls, queue_name: str, task_name: str, message: str) -> 'MessageLog':
        with contextlib.closing(db.get().cursor()) as cursor:
            cursor.execute("""
            INSERT INTO message_logs (queue_name, task_name, message)
            VALUES (?, ?, ?)
            ;""", (queue_name, task_name, message, ))
            # If SQLite3 supports "returning", I can use it here.
            id = cursor.lastrowid

        return cls.get_by_id(id)

    @classmethod
    def list_by_queue_name_and_task_name(
            cls, queue_name: str, task_name: str) -> Iterator['MessageLog']:
        with contextlib.closing(db.get().cursor()) as cursor:
            cursor.execute("""
            SELECT *
            FROM message_logs
            WHERE
                message_logs.queue_name = ? AND
                message_logs.task_name = ?
            ORDER BY message_logs.created_at DESC
            ;""", (queue_name, task_name, ))

            return (cls.from_sqlite3_row(row) for row in cursor.fetchall())

    @classmethod
    def from_sqlite3_row(cls, row: Optional[sqlite3.Row]) -> Optional['MessageLog']:
        return cls(**row) if row else None  # type: ignore

    @classmethod
    def eliminate(cls, queue_name: str, task_name: str) -> None:
        with contextlib.closing(db.get().cursor()) as cursor:
            cursor.execute("""
            DELETE FROM message_logs
            WHERE
                queue_name = :queue_name AND
                task_name = :task_name AND
                id NOT IN (
                    SELECT id
                    FROM message_logs
                    WHERE
                        queue_name = :queue_name AND
                        task_name = :task_name
                    ORDER BY created_at DESC
                    LIMIT 1000
                )
            ;
            """, {
                'queue_name': queue_name,
                'task_name': task_name,
            })
