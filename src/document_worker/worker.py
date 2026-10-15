"""Async worker that correlates model output to a fenced durable job."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable

from .agent import DocumentFacts, DocumentExtraction, ExtractionError
from .store import JobStore

Processor = Callable[[str], Awaitable[DocumentFacts | DocumentExtraction]]


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
            if isinstance(result, DocumentExtraction):
                facts = DocumentFacts.model_validate(result.facts)
                model_observation = result.model_observation
            else:
                facts = DocumentFacts.model_validate(result)
                model_observation = {"source": "processor_without_metadata", "requested_model": None,
                                     "response_models": None, "usage_events": None, "cost_usd": None}
            complete = (facts.document_type in {"invoice", "receipt"} and not facts.concerns
                        and all((facts.issuer, facts.currency, facts.total_cents is not None)))
            status = "completed" if complete else "needs_review"
            payload = json.dumps({
                "job_id": job["job_id"],
                "document_id": job["document_id"],
                "attempt": job["attempt"],
                "facts": facts.model_dump(),
                "model_observation": model_observation,
            })
            committed = self.store.finish(job["job_id"], job["lease_token"], payload, status)
        except asyncio.CancelledError:
            # A killed worker leaves the lease to expire; a newer worker gets a new token.
            raise
        except Exception as error:
            observed = json.dumps({"job_id": job["job_id"], "document_id": job["document_id"], "attempt": job["attempt"],
                                   "model_observation": error.model_observation}) if isinstance(error, ExtractionError) else None
            committed = self.store.finish(job["job_id"], job["lease_token"], observed, status="failed", error=str(error))
        current = self.store.get(job["job_id"])
        return {**current, "worker_attempt": job["attempt"], "commit_accepted": committed} if current is not None else None

    async def run_forever(self, poll_seconds: float = 1) -> None:
        while True:
            result = await self.run_once()
            if result is None:
                await asyncio.sleep(poll_seconds)
