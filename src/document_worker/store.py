"""SQLite job journal with idempotent submission and fenced worker leases."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Callable

MAX_ATTEMPTS = 3


class JobStore:
    def __init__(self, path: str | Path, clock: Callable[[], float] = time.time):
        if str(path)==':memory:':raise ValueError('a durable journal file is required')
        file=Path(path).resolve();file.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
        try:
            descriptor=os.open(file,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600);os.close(descriptor)
        except FileExistsError:pass
        self.path = str(file)
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
                    updated_at REAL NOT NULL,
                    dataset_json TEXT
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
                    lease_token TEXT,
                    observation_json TEXT,
                    PRIMARY KEY (job_id, attempt)
                );
            """)
            db.execute("BEGIN IMMEDIATE")
            job_columns={row['name'] for row in db.execute('PRAGMA table_info(jobs)')}
            if 'dataset_json' not in job_columns:db.execute('ALTER TABLE jobs ADD COLUMN dataset_json TEXT')
            columns={row['name'] for row in db.execute('PRAGMA table_info(job_attempts)')}
            for column in ('lease_token','observation_json'):
                if column not in columns:db.execute(f'ALTER TABLE job_attempts ADD COLUMN {column} TEXT')

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

    def submit(self, request_key: str, document_id: str, document_text: str, dataset: dict|None=None) -> dict:
        if not all(isinstance(value,str) and value.strip() for value in (request_key,document_id,document_text)):
            raise ValueError("request_key, document_id and document_text are required")
        if len(request_key)>200 or len(document_id)>200:raise ValueError('request_key/document_id exceeds 200 characters')
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
                if dataset is not None and row['dataset_json']!=json.dumps(dataset,sort_keys=True,allow_nan=False):raise ValueError('request_key belongs to another dataset identity')
                return dict(row)
            job_id = str(uuid.uuid4())
            db.execute("""INSERT INTO jobs
                (job_id, request_key, document_id, document_text, content_sha256, status, created_at, updated_at, dataset_json)
                VALUES (?, ?, ?, ?, ?, 'queued', ?, ?, ?)""",
                (job_id, request_key, document_id, document_text, digest, now, now, json.dumps(dataset,sort_keys=True,allow_nan=False) if dataset is not None else None))
            return self.get(job_id, db)

    def import_human(self,job_id: str,facts: dict,status: str) -> dict:
        """Trusted synthetic authoring seam: human-entered facts, zero model claims."""
        if status not in {'completed','needs_review'}:raise ValueError('invalid human import status')
        with self._connect() as db:
            db.execute('BEGIN IMMEDIATE');row=self.get(job_id,db)
            if row is None or row['status']!='queued' or row['attempt']!=0:raise ValueError('human import requires a fresh unclaimed job')
            payload=json.dumps({'job_id':job_id,'document_id':row['document_id'],'attempt':0,'facts':facts,
                'model_observation':{'source':'human_import','requested_model':None,'response_models':None,'usage_events':None,'cost_usd':None}},allow_nan=False)
            db.execute('UPDATE jobs SET status=?,result_json=?,updated_at=? WHERE job_id=?',(status,payload,self.clock(),job_id))
            return self.get(job_id,db)

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
                error='lease expired; inspect attempt observations for provider outcome', finished_at=?
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
            db.execute("""INSERT INTO job_attempts (job_id, attempt, status, started_at, lease_token)
                SELECT job_id, attempt, 'running', ?, lease_token FROM jobs WHERE job_id=?""", (now, row["job_id"]))
            return self.get(row["job_id"], db)

    def observe(self,job_id: str,attempt: int,lease_token: str,observation: dict,error: str|None=None) -> bool:
        """Persist an owned attempt observation even after its facts are fenced out."""
        raw=json.dumps(observation,allow_nan=False)
        with self._connect() as db:
            db.execute('BEGIN IMMEDIATE')
            changed=db.execute("UPDATE job_attempts SET observation_json=?, error=COALESCE(?,error) WHERE job_id=? AND attempt=? AND lease_token=?",
                (raw,error,job_id,attempt,lease_token)).rowcount
            return changed==1

    def finish(self, job_id: str, lease_token: str, result_json: str | None = None,
               status: str = "completed", error: str | None = None) -> bool:
        if status not in {"completed", "needs_review", "failed"}:
            raise ValueError("invalid terminal status")
        now = self.clock()
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = self.get(job_id, db)
            if row is None or row["status"] != "running" or row["lease_token"] != lease_token or row["lease_until"]<=now:
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
