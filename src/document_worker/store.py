"""SQLite job journal with idempotent submission and fenced worker leases."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Callable

MAX_ATTEMPTS = 3


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
                CREATE TABLE IF NOT EXISTS job_attempts (
                    job_id TEXT NOT NULL,
                    attempt INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    result_json TEXT,
                    error TEXT,
                    started_at REAL NOT NULL,
                    finished_at REAL,
                    PRIMARY KEY (job_id, attempt)
                );
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
            if row is None:
                return None
            result = dict(row)
            result["attempts"] = [dict(item) for item in db.execute(
                "SELECT * FROM job_attempts WHERE job_id=? ORDER BY attempt", (job_id,))]
            return result
        with self._connect() as owned:
            owned.execute("BEGIN")
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
            if row["attempt"] >= MAX_ATTEMPTS:
                raise ValueError("retry limit reached")
            db.execute("""UPDATE jobs SET status='queued', error=NULL, result_json=NULL, lease_token=NULL,
                lease_until=NULL, cancel_requested=0, updated_at=? WHERE job_id=?""", (now, job_id))
            return self.get(job_id, db)

    def claim(self, lease_seconds: int = 300) -> dict | None:
        if isinstance(lease_seconds, bool) or not isinstance(lease_seconds, int) or lease_seconds <= 0:
            raise ValueError("lease_seconds must be a positive integer")
        now = self.clock()
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute("""UPDATE job_attempts SET
                status=CASE WHEN EXISTS (SELECT 1 FROM jobs j WHERE j.job_id=job_attempts.job_id AND j.cancel_requested=1)
                            THEN 'cancelled' ELSE 'lease_expired' END,
                error='lease expired; provider outcome unknown', finished_at=?
                WHERE status='running' AND EXISTS (
                    SELECT 1 FROM jobs j WHERE j.job_id=job_attempts.job_id AND j.attempt=job_attempts.attempt
                    AND j.status='running' AND j.lease_until<=?)""", (now, now))
            db.execute("""UPDATE jobs SET status='cancelled', lease_token=NULL, lease_until=NULL, updated_at=?
                WHERE status='running' AND lease_until<=? AND cancel_requested=1""", (now, now))
            db.execute("""UPDATE jobs SET status='failed', lease_token=NULL, lease_until=NULL,
                error='worker attempt limit reached', updated_at=?
                WHERE cancel_requested=0 AND attempt>=? AND
                    ((status='running' AND lease_until<=?) OR status='queued')""", (now, MAX_ATTEMPTS, now))
            db.execute("""UPDATE jobs SET status='queued', lease_token=NULL, lease_until=NULL, updated_at=?
                WHERE status='running' AND lease_until<=? AND cancel_requested=0 AND attempt<?""", (now, now, MAX_ATTEMPTS))
            row = db.execute("SELECT job_id FROM jobs WHERE status='queued' AND attempt<? ORDER BY created_at, job_id LIMIT 1", (MAX_ATTEMPTS,)).fetchone()
            if not row:
                return None
            token = str(uuid.uuid4())
            db.execute("""UPDATE jobs SET status='running', attempt=attempt+1,
                lease_token=?, lease_until=?, updated_at=? WHERE job_id=?""",
                (token, now + lease_seconds, now, row["job_id"]))
            db.execute("""INSERT INTO job_attempts (job_id, attempt, status, started_at)
                SELECT job_id, attempt, 'running', ? FROM jobs WHERE job_id=?""", (now, row["job_id"]))
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
            attempt_result = result_json
            if row["cancel_requested"]:
                observed = json.loads(result_json).get("model_observation") if result_json else None
                attempt_result = json.dumps({"job_id": job_id, "document_id": row["document_id"],
                    "attempt": row["attempt"], "model_observation": observed}) if observed is not None else None
                status, result_json, error = "cancelled", None, None
            db.execute("""UPDATE job_attempts SET status=?, result_json=?, error=?, finished_at=?
                WHERE job_id=? AND attempt=?""", (status, attempt_result, error, now, job_id, row["attempt"]))
            db.execute("""UPDATE jobs SET status=?, result_json=?, error=?, lease_token=NULL,
                lease_until=NULL, updated_at=? WHERE job_id=?""",
                (status, result_json, error, now, job_id))
            return True
