"""SQLite job journal with idempotent submission and fenced worker leases."""

from __future__ import annotations

import hashlib
import sqlite3
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Callable


class JobStore:
    def __init__(self, path: str | Path, clock: Callable[[], float] = time.time):
        self.path = str(path)
        self.clock = clock
        with self._connect() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript("""
                CREATE TABLE IF NOT EXISTS jobs (
                    job_id TEXT PRIMARY KEY,
                    request_key TEXT NOT NULL UNIQUE,
                    document_id TEXT NOT NULL,
                    document_text TEXT NOT NULL,
                    content_sha256 TEXT NOT NULL,
                    status TEXT NOT NULL,
                    attempt INTEGER NOT NULL DEFAULT 0,
                    lease_token TEXT,
                    lease_until REAL,
                    cancel_requested INTEGER NOT NULL DEFAULT 0,
                    result_json TEXT,
                    error TEXT,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS jobs_status_created ON jobs(status, created_at);
            """)

    @contextmanager
    def _connect(self):
        db = sqlite3.connect(self.path, timeout=10, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA busy_timeout=10000")
        try:
            yield db
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    def submit(self, request_key: str, document_id: str, document_text: str) -> dict:
        if not request_key.strip() or not document_id.strip() or not document_text.strip():
            raise ValueError("request_key, document_id and document_text are required")
        if len(document_text) > 100_000:
            raise ValueError("document_text exceeds 100000 characters")
        digest = hashlib.sha256(document_text.encode()).hexdigest()
        now = self.clock()
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM jobs WHERE request_key=?", (request_key,)).fetchone()
            if row:
                if row["document_id"] != document_id or row["content_sha256"] != digest:
                    raise ValueError("request_key was already used for a different document")
                return dict(row)
            job_id = str(uuid.uuid4())
            db.execute("""INSERT INTO jobs
                (job_id, request_key, document_id, document_text, content_sha256, status, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, 'queued', ?, ?)""",
                (job_id, request_key, document_id, document_text, digest, now, now))
            return self.get(job_id, db)

    def get(self, job_id: str, db: sqlite3.Connection | None = None) -> dict | None:
        if db is not None:
            row = db.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone()
            return dict(row) if row else None
        with self._connect() as owned:
            return self.get(job_id, owned)

    def list_jobs(self) -> list[dict]:
        with self._connect() as db:
            return [dict(row) for row in db.execute("SELECT * FROM jobs ORDER BY created_at, job_id")]

    def cancel(self, job_id: str) -> dict | None:
        now = self.clock()
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = self.get(job_id, db)
            if row is None:
                return None
            if row["status"] == "queued":
                db.execute("UPDATE jobs SET status='cancelled', cancel_requested=1, updated_at=? WHERE job_id=?", (now, job_id))
            elif row["status"] == "running":
                db.execute("UPDATE jobs SET cancel_requested=1, updated_at=? WHERE job_id=?", (now, job_id))
            return self.get(job_id, db)

    def retry(self, job_id: str) -> dict | None:
        now = self.clock()
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = self.get(job_id, db)
            if row is None:
                return None
            if row["status"] != "failed":
                raise ValueError("only failed jobs can be retried")
            if row["attempt"] >= 3:
                raise ValueError("retry limit reached")
            db.execute("""UPDATE jobs SET status='queued', error=NULL, lease_token=NULL,
                lease_until=NULL, cancel_requested=0, updated_at=? WHERE job_id=?""", (now, job_id))
            return self.get(job_id, db)

    def claim(self, lease_seconds: int = 300) -> dict | None:
        now = self.clock()
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute("""UPDATE jobs SET status='cancelled', lease_token=NULL, lease_until=NULL, updated_at=?
                WHERE status='running' AND lease_until<=? AND cancel_requested=1""", (now, now))
            db.execute("""UPDATE jobs SET status='queued', lease_token=NULL, lease_until=NULL, updated_at=?
                WHERE status='running' AND lease_until<=? AND cancel_requested=0""", (now, now))
            row = db.execute("SELECT job_id FROM jobs WHERE status='queued' ORDER BY created_at, job_id LIMIT 1").fetchone()
            if not row:
                return None
            token = str(uuid.uuid4())
            db.execute("""UPDATE jobs SET status='running', attempt=attempt+1,
                lease_token=?, lease_until=?, updated_at=? WHERE job_id=?""",
                (token, now + lease_seconds, now, row["job_id"]))
            return self.get(row["job_id"], db)

    def finish(self, job_id: str, lease_token: str, result_json: str | None = None,
               status: str = "completed", error: str | None = None) -> bool:
        if status not in {"completed", "needs_review", "failed"}:
            raise ValueError("invalid terminal status")
        now = self.clock()
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = self.get(job_id, db)
            if row is None or row["status"] != "running" or row["lease_token"] != lease_token:
                return False
            if row["cancel_requested"]:
                status, result_json, error = "cancelled", None, None
            db.execute("""UPDATE jobs SET status=?, result_json=?, error=?, lease_token=NULL,
                lease_until=NULL, updated_at=? WHERE job_id=?""",
                (status, result_json, error, now, job_id))
            return True
