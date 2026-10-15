# Asynchronous document processing agent

A durable job worker for extracting invoice and receipt facts. It uses [Google ADK's runner](https://github.com/google/adk-python/blob/main/docs/guides/runners/runner/index.md) and a Gemini model for structured extraction, with SQLite handling submission, completion, cancellation and restart recovery. Each result carries the original job and document IDs.

## Run

Requires Python 3.12+. Local tests call ADK through a stub model and use no provider account.

```sh
uv sync --extra test
uv run pytest -q
uv run python -m document_worker --db jobs.db submit \
  --key delivery-1048 --document-id invoice-a --file fixtures/invoice-a.txt
uv run python -m document_worker --db jobs.db list
```

To process queued jobs with a real Gemini model, set `GOOGLE_API_KEY` and `GEMINI_MODEL` to an enabled model, then run:

```sh
uv run python -m document_worker --db jobs.db work --once
uv run python -m document_worker --db jobs.db work
```

`get JOB_ID`, `cancel JOB_ID`, and `retry JOB_ID` use the same `--db` option. `work` polls continuously; `--once` claims at most one job. The CLI omits the document body and lease token from its output. `fixtures/manifest.json` identifies dataset `synthetic-document-jobs` version `1`, with starter, busy, ambiguous and boundary profiles. Its five fictional text documents include complete invoices, a receipt, unreadable fields and a contradictory correction. See [scenario briefs](scenarios/README.md) for exact inputs, completion signals and reset ownership.

## Job behavior

- A repeated idempotency key with the same document returns the original job. Reusing it for different content is rejected.
- Only one worker can claim a queued job. A lease token fences an older worker if its lease expires and another worker reclaims the job.
- A queued cancellation stops the job immediately. A running cancellation marks it for cancellation; any late model result is discarded at commit time. The provider call may still finish, so cancellation is cooperative.
- If a worker is interrupted, its lease expires and a later worker retries that same job. An old worker cannot commit after a new lease is issued. Lease durations must be positive integers.
- Model or validation failure marks the job failed. `retry` requeues it with the same ID. Explicit retries and expired-lease recovery share a maximum of three worker claims; a job that exhausts that budget stops as failed. The `attempt` field counts claims, which may be interrupted before a model completes; it is not a count of successful provider executions.
- Missing issuer, currency or total, an unknown document type, or any model-reported concern produces `needs_review`; the worker does not silently mark contradictory facts complete.
- Each extraction deletes its ephemeral ADK session on success, failure or asyncio cancellation. The CLI closes its runner when its worker invocation exits normally or is cancelled. Abrupt process termination leaves journal recovery to lease expiry.

The SQLite journal stores source text and result data for this synthetic example. Use a protected database and an explicit retention policy before adapting it for real documents. This example accepts extracted text, not PDF or OCR input.

## Evidence and integration

`uv run --locked --extra test pytest -q` passes 19 local cases, including the actual ADK runner with a stub model, correlated results from the busy profile, cancellation and fencing, three-attempt recovery exhaustion, invalid leases, ambiguous facts, and session cleanup after success/failure/cancellation. Regression checks reproduced unbounded lease recovery and accumulating ADK sessions before the fixes. These are offline results and do not prove Gemini output quality or a live provider call. Live Gemini processing and exact field accuracy need an account key and measured results. A thin Firedrill connection will be added after the new Firedrill release is qualified; no retired client is used.

Dependencies are pinned in `uv.lock`, including `google-adk==2.12.0`.

Explore related applications in the [Firedrill example catalog](https://github.com/firedrill-tools/firedrill-example-catalog). Product and setup references: [Firedrill](https://firedrill.run/) and the [official developer documentation](https://docs.firedrill.run/). Use documentation matching the qualified release; this repository records its current integration limits above.
