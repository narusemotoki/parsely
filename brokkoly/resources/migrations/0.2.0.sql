BEGIN;

CREATE TABLE migrations (
    version TEXT PRIMARY KEY
);

INSERT INTO migrations (version) VALUES ('0.2.0');

CREATE TABLE message_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    queue_name TEXT NOT NULL,
    task_name TEXT NOT NULL,
    message TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX message_logs_queue_name_task_name_created_at
ON message_logs(queue_name, task_name, created_at);

COMMIT;
