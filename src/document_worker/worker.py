"""Async worker that correlates model output to a fenced durable job."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable

from .agent import DocumentFacts
from .store import JobStore

Processor = Callable[[str], Awaitable[DocumentFacts]]


class DocumentWorker:
    def __init__(self, store: JobStore, processor: Processor, timeout_seconds: float = 120):
        self.store = store
        self.processor = processor
        self.timeout_seconds = timeout_seconds

    async def run_once(self, lease_seconds: int = 300) -> dict | None:
        job = self.store.claim(lease_seconds)
        if job is None:
            return None
        try:
            result = await asyncio.wait_for(
                self.processor(job["document_text"]), timeout=self.timeout_seconds
            )
            facts = DocumentFacts.model_validate(result)
            status = "completed" if all((facts.issuer, facts.currency, facts.total_cents is not None)) else "needs_review"
            payload = json.dumps({
                "job_id": job["job_id"],
                "document_id": job["document_id"],
                "facts": facts.model_dump(),
            })
            self.store.finish(job["job_id"], job["lease_token"], payload, status)
        except asyncio.CancelledError:
            # A killed worker leaves the lease to expire; a newer worker gets a new token.
            raise
        except Exception as error:
            self.store.finish(job["job_id"], job["lease_token"], status="failed", error=str(error))
        return self.store.get(job["job_id"])

    async def run_forever(self, poll_seconds: float = 1) -> None:
        while True:
            result = await self.run_once()
            if result is None:
                await asyncio.sleep(poll_seconds)
