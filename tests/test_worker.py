import asyncio
import json

import pytest
from google.adk.models.base_llm import BaseLlm
from google.adk.models.llm_response import LlmResponse
from google.genai import types

from document_worker.agent import ADKExtractor, DocumentFacts
from document_worker.store import JobStore
from document_worker.worker import DocumentWorker


class Clock:
    now = 1000.0

    def __call__(self):
        return self.now


class FakeDocumentModel(BaseLlm):
    model: str = "fixture-model"

    async def generate_content_async(self, llm_request, stream=False):
        # This exercises the actual ADK runner and structured-output state path.
        result = DocumentFacts(
            document_type="invoice", issuer="Northstar Paper Supply",
            document_number="INV-1048", currency="USD", total_cents=5000,
            due_date="2026-11-09",
        )
        yield LlmResponse(content=types.Content(
            role="model", parts=[types.Part.from_text(text=result.model_dump_json())]
        ))


def test_adk_runner_structured_output_without_network():
    result = asyncio.run(ADKExtractor(model=FakeDocumentModel())("synthetic invoice text"))
    assert result.document_number == "INV-1048"
    assert result.total_cents == 5000


def test_submission_is_idempotent_and_conflict_detected(tmp_path):
    store = JobStore(tmp_path / "jobs.db")
    first = store.submit("delivery-1", "invoice-a", "Total $50")
    repeated = store.submit("delivery-1", "invoice-a", "Total $50")
    assert first["job_id"] == repeated["job_id"]
    assert len(store.list_jobs()) == 1
    with pytest.raises(ValueError, match="different document"):
        store.submit("delivery-1", "invoice-b", "Total $18")


def test_result_is_correlated_and_missing_fields_need_review(tmp_path):
    store = JobStore(tmp_path / "jobs.db")
    one = store.submit("a", "invoice-a", "A")
    two = store.submit("b", "unreadable-c", "B")

    async def processor(text):
        if text == "A":
            return DocumentFacts(document_type="invoice", issuer="Acme", currency="USD", total_cents=5000)
        return DocumentFacts(document_type="unknown", issuer=None, currency=None, total_cents=None,
                             concerns=["unreadable total"])

    worker = DocumentWorker(store, processor)
    first = asyncio.run(worker.run_once())
    second = asyncio.run(worker.run_once())
    assert first["status"] == "completed"
    assert second["status"] == "needs_review"
    assert json.loads(first["result_json"])["job_id"] == one["job_id"]
    assert json.loads(second["result_json"])["document_id"] == two["document_id"]
    assert store.get(one["job_id"])["attempt"] == 1


def test_running_cancellation_discards_late_result(tmp_path):
    store = JobStore(tmp_path / "jobs.db")
    job = store.submit("cancel", "invoice-a", "A")
    started = asyncio.Event()
    release = asyncio.Event()

    async def processor(_text):
        started.set()
        await release.wait()
        return DocumentFacts(document_type="invoice", issuer="Acme", currency="USD", total_cents=5000)

    async def scenario():
        task = asyncio.create_task(DocumentWorker(store, processor).run_once())
        await started.wait()
        assert store.cancel(job["job_id"])["cancel_requested"] == 1
        release.set()
        await task

    asyncio.run(scenario())
    final = store.get(job["job_id"])
    assert final["status"] == "cancelled"
    assert final["result_json"] is None


def test_interrupted_worker_is_reclaimed_and_old_result_is_fenced(tmp_path):
    clock = Clock()
    store = JobStore(tmp_path / "jobs.db", clock=clock)
    job = store.submit("crash", "invoice-a", "A")
    first = store.claim(lease_seconds=10)
    assert store.claim() is None
    clock.now += 11
    replacement = store.claim(lease_seconds=10)
    assert replacement["job_id"] == job["job_id"]
    assert replacement["attempt"] == 2
    assert replacement["lease_token"] != first["lease_token"]
    assert store.finish(job["job_id"], first["lease_token"], "{}") is False
    assert store.finish(job["job_id"], replacement["lease_token"], "{}") is True
    assert store.get(job["job_id"])["status"] == "completed"


def test_failure_retries_same_job_with_limit(tmp_path):
    store = JobStore(tmp_path / "jobs.db")
    job = store.submit("fail", "invoice-a", "A")

    async def broken(_text):
        raise RuntimeError("synthetic model outage")

    failed = asyncio.run(DocumentWorker(store, broken).run_once())
    assert failed["status"] == "failed"
    assert failed["error"] == "synthetic model outage"
    retried = store.retry(job["job_id"])
    assert retried["status"] == "queued"
    assert retried["job_id"] == job["job_id"]


def test_queued_cancellation_prevents_claim(tmp_path):
    store = JobStore(tmp_path / "jobs.db")
    job = store.submit("queued", "invoice-a", "A")
    assert store.cancel(job["job_id"])["status"] == "cancelled"
    assert store.claim() is None
