import asyncio
import json
import hashlib
from pathlib import Path

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
    fail: bool = False
    delay_seconds: float = 0

    async def generate_content_async(self, llm_request, stream=False):
        # This exercises the actual ADK runner and structured-output state path.
        if self.fail:
            raise RuntimeError("synthetic model failure")
        if self.delay_seconds:
            await asyncio.sleep(self.delay_seconds)
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


def test_repeated_worker_interruptions_share_the_three_attempt_limit(tmp_path):
    clock = Clock()
    store = JobStore(tmp_path / "jobs.db", clock=clock)
    job = store.submit("repeated-crash", "invoice-a", "A")
    for attempt in range(1, 4):
        claimed = store.claim(lease_seconds=10)
        assert claimed["job_id"] == job["job_id"]
        assert claimed["attempt"] == attempt
        clock.now += 11
    assert store.claim() is None
    final = store.get(job["job_id"])
    assert final["status"] == "failed"
    assert final["attempt"] == 3
    assert "limit" in final["error"]
    with pytest.raises(ValueError, match="limit"):
        store.retry(job["job_id"])


def test_adk_sessions_do_not_accumulate_after_completed_documents():
    async def scenario():
        extractor = ADKExtractor(model=FakeDocumentModel())
        try:
            await extractor("first synthetic document")
            await extractor("second synthetic document")
            remaining = await extractor.runner.session_service.list_sessions(app_name="document_processing", user_id="worker")
            assert remaining.sessions == []
        finally:
            await extractor.close()
    asyncio.run(scenario())


def test_adk_session_is_removed_after_model_failure():
    async def scenario():
        extractor = ADKExtractor(model=FakeDocumentModel(fail=True))
        try:
            with pytest.raises(RuntimeError, match="synthetic model failure"):
                await extractor("synthetic failing document")
            remaining = await extractor.runner.session_service.list_sessions(app_name="document_processing", user_id="worker")
            assert remaining.sessions == []
        finally:
            await extractor.close()
    asyncio.run(scenario())


def test_adk_session_is_removed_after_cancellation():
    async def scenario():
        extractor = ADKExtractor(model=FakeDocumentModel(delay_seconds=60))
        task = asyncio.create_task(extractor("synthetic interrupted document"))
        try:
            async def wait_for_session():
                while not (await extractor.runner.session_service.list_sessions(app_name="document_processing", user_id="worker")).sessions:
                    await asyncio.sleep(0)
            await asyncio.wait_for(wait_for_session(), timeout=2)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            remaining = await extractor.runner.session_service.list_sessions(app_name="document_processing", user_id="worker")
            assert remaining.sessions == []
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            await extractor.close()
    asyncio.run(scenario())


@pytest.mark.parametrize("lease_seconds", [0, -1, 1.5, True])
def test_invalid_lease_cannot_consume_an_attempt(tmp_path, lease_seconds):
    store = JobStore(tmp_path / "jobs.db")
    job = store.submit("invalid-lease", "invoice-a", "A")
    with pytest.raises(ValueError, match="positive integer"):
        store.claim(lease_seconds=lease_seconds)
    assert store.get(job["job_id"])["attempt"] == 0
    assert store.get(job["job_id"])["status"] == "queued"


def test_cancelled_third_attempt_stays_cancelled_when_lease_expires(tmp_path):
    clock = Clock(); store = JobStore(tmp_path / "jobs.db", clock=clock)
    job = store.submit("last-cancel", "invoice-a", "A")
    for _ in range(3):
        store.claim(lease_seconds=10)
        clock.now += 11
    store.cancel(job["job_id"])
    assert store.claim() is None
    assert store.get(job["job_id"])["status"] == "cancelled"


@pytest.mark.parametrize("document_type,concerns", [("invoice", ["two contradictory totals"]), ("unknown", [])])
def test_ambiguous_facts_cannot_be_marked_completed(tmp_path, document_type, concerns):
    store = JobStore(tmp_path / "jobs.db")
    store.submit("ambiguous", "ambiguous-e", "Two conflicting totals")
    async def processor(_text):
        return DocumentFacts(document_type=document_type, issuer="Synthetic Vendor", currency="USD", total_cents=5000, concerns=concerns)
    result = asyncio.run(DocumentWorker(store, processor).run_once())
    assert result["status"] == "needs_review"
    assert json.loads(result["result_json"])["facts"]["concerns"] == concerns


def test_busy_fixture_profile_preserves_source_and_result_correlations(tmp_path):
    fixtures = Path(__file__).parents[1] / "fixtures"
    manifest = json.loads((fixtures / "manifest.json").read_text())
    assert manifest["dataset_id"] == "synthetic-document-jobs" and manifest["version"] == 1
    facts = {
        "invoice-a": DocumentFacts(document_type="invoice", issuer="Northstar Paper Supply", currency="USD", total_cents=5000),
        "receipt-b": DocumentFacts(document_type="receipt", issuer="Synthetic Corner Shop", currency="CAD", total_cents=1845),
        "invoice-d": DocumentFacts(document_type="invoice", issuer="Synthetic Cedar Office Supply", currency="CAD", total_cents=3000),
    }
    store = JobStore(tmp_path / "busy.db"); expected = {}
    for document_id in manifest["profiles"]["busy"]:
        text = (fixtures / manifest["documents"][document_id]).read_text()
        job = store.submit("busy-" + document_id, document_id, text)
        assert job["content_sha256"] == hashlib.sha256(text.encode()).hexdigest()
        expected[text] = facts[document_id]
    async def processor(text):
        return expected[text]
    worker = DocumentWorker(store, processor)
    async def drain():
        while await worker.run_once() is not None:
            pass
    asyncio.run(drain())
    rows = store.list_jobs()
    assert len(rows) == 3
    for job in rows:
        assert job["status"] == "completed"
        result = json.loads(job["result_json"])
        assert result["job_id"] == job["job_id"]
        assert result["document_id"] == job["document_id"]
        assert result["facts"] == facts[job["document_id"]].model_dump()
