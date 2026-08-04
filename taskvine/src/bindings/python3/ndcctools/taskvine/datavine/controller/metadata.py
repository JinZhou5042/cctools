"""Durable, incremental Controller metadata store."""

import pickle
import sqlite3
import time
from pathlib import Path


class MetadataStore:
    VERSION = 1

    def __init__(self, root):
        root = Path(root)
        root.mkdir(parents=True, exist_ok=True)
        self.path = root / "controller-metadata.sqlite3"
        self._database = sqlite3.connect(self.path, check_same_thread=False)
        self._database.execute("PRAGMA journal_mode=WAL")
        self._database.execute("PRAGMA synchronous=NORMAL")
        self._database.execute("PRAGMA foreign_keys=ON")
        self._commits = 0
        self._records_written = 0
        self._commit_seconds = 0.0
        self._database.executescript(
            """
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value BLOB NOT NULL
            );
            CREATE TABLE IF NOT EXISTS records (
                kind TEXT NOT NULL,
                id INTEGER NOT NULL,
                sequence INTEGER,
                value BLOB NOT NULL,
                PRIMARY KEY (kind, id)
            );
            """
        )
        version = self._database.execute(
            "SELECT value FROM metadata WHERE key='version'"
        ).fetchone()
        if version is None:
            self._database.execute(
                "INSERT INTO metadata(key,value) VALUES('version',?)",
                (self._encode(self.VERSION),),
            )
            self._database.commit()
        elif self._decode(version[0]) != self.VERSION:
            raise RuntimeError("unsupported Controller metadata version")

    @staticmethod
    def _encode(value):
        return pickle.dumps(value, protocol=5)

    @staticmethod
    def _decode(value):
        return pickle.loads(value)

    def commit(self, records, task_states, data_states):
        records = tuple(records)
        task_states = tuple(task_states)
        data_states = tuple(data_states)
        started = time.perf_counter()
        with self._database:
            self._database.executemany(
                """
                INSERT INTO records(kind,id,sequence,value) VALUES(?,?,?,?)
                ON CONFLICT(kind,id) DO UPDATE SET value=excluded.value
                """,
                (
                    (kind, int(record_id), sequence, self._encode(value))
                    for kind, record_id, sequence, value in records
                ),
            )
            self._database.executemany(
                """
                INSERT INTO records(kind,id,sequence,value) VALUES('task-state',?,?,?)
                ON CONFLICT(kind,id) DO UPDATE SET value=excluded.value
                """,
                (
                    (int(task_id), None, self._encode(state))
                    for task_id, state in task_states
                ),
            )
            self._database.executemany(
                """
                INSERT INTO records(kind,id,sequence,value) VALUES('data-state',?,?,?)
                ON CONFLICT(kind,id) DO UPDATE SET value=excluded.value
                """,
                (
                    (int(data_id), None, self._encode(state))
                    for data_id, state in data_states
                ),
            )
        self._commits += 1
        self._records_written += (
            len(records) + len(task_states) + len(data_states)
        )
        self._commit_seconds += time.perf_counter() - started

    def load(self, kind):
        rows = self._database.execute(
            """
            SELECT id,value FROM records WHERE kind=?
            ORDER BY COALESCE(sequence,id),id
            """,
            (str(kind),),
        )
        return tuple(
            (int(record_id), self._decode(value))
            for record_id, value in rows
        )

    def has_records(self):
        return self._database.execute(
            "SELECT EXISTS(SELECT 1 FROM records)"
        ).fetchone()[0] == 1

    def snapshot(self):
        rows = self._database.execute(
            "SELECT kind,COUNT(*) FROM records GROUP BY kind"
        )
        wal_path = Path(f"{self.path}-wal")
        return {
            "path": str(self.path),
            "records": {kind: count for kind, count in rows},
            "commits": self._commits,
            "records_written": self._records_written,
            "commit_seconds": self._commit_seconds,
            "database_bytes": self.path.stat().st_size,
            "wal_bytes": wal_path.stat().st_size if wal_path.exists() else 0,
        }

    def close(self):
        if self._database is None:
            return
        self._database.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        self._database.close()
        self._database = None
