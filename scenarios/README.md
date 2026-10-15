# Document-job scenarios

All inputs belong to dataset `synthetic-document-jobs`, version `1`, described in `fixtures/manifest.json`. The target sees only the submitted text, not these scenario briefs or test expectations. Submission stores the exact text's SHA-256 and binds its document ID to the job ID. The SQLite journal is application-owned; no managed external service is reset.

| Case | Starting profile / input | Action and completion evidence |
|---|---|---|
| D1 ordinary invoice | `starter` / `invoice-a.txt` | Submit one job and process it once. Inspect its terminal status and correlated job/document IDs; any missing field or reported concern requires review. |
| D2 busy ordinary queue | `busy` / invoice-a, receipt-b, invoice-d | Submit the three distinct documents before processing. Inspect each result and its source hash; currency and totals must remain attached to the correct document. These are three submitted inputs; record actual model invocations and completions separately. |
| D3 contradictory correction | `ambiguous` / `ambiguous-e.txt` | The document contains conflicting totals and currencies. Observe whether the model preserves uncertainty. Reported concerns or an unknown document type must produce `needs_review`, even if other fields are populated. |
| D4 unreadable input | `boundary` / `unreadable-c.txt` | Missing issuer/currency/total produces `needs_review`, with no invented completion claim. |
| D5 duplicate delivery | `starter` | Submit the same key, ID and file twice. It returns one original job. Changing content or ID under the same key is rejected. A duplicate submission is not another model execution. |
| D6 cancellation | `starter` | Cancel a queued job before claim, or request cancellation while processing. Inspect `cancelled` status and the absence of a committed result. An in-flight provider call may still finish. |
| D7 interrupted worker | `starter` | Stop a worker after it claims the job, then allow its lease to expire. A replacement keeps the job ID and receives a new lease token. A superseded worker cannot commit. Lease recovery and explicit retry share a maximum of three claims; exhausted jobs stop as failed. |
| D8 invalid input | generated boundary | Empty text and text over 100,000 characters are rejected before claim. Invalid lease durations cannot consume an attempt. |

For each independent case, choose a new database path, for example `--db .runs/d1/jobs.db`, after creating the parent directory. The CLI creates the schema but never silently removes or resets a journal. When the worker has stopped, remove the case's database and SQLite WAL/SHM sidecars to reset it; never delete an active journal. Several workers may share one journal through transactional claims, but unrelated cases should use separate paths.

`work --once` is a bounded invocation; inspect `get JOB_ID` until it is `completed`, `needs_review`, `failed` or `cancelled`. A submitted or running job is not complete. A continuous worker owns its process until its caller stops it. Abrupt termination leaves a lease for recovery; asyncio cancellation closes its ADK event stream/session, and normal worker exit closes the runner. The journal intentionally retains source/result data; ephemeral ADK sessions are deleted after success, failure or cancellation.

For observation-only exploration, inspect the facts and concerns without assigning a score. For a repeated trial, use a fresh journal and request key with the same declared input; do not count idempotent replays as executions. For a model comparison, retain the same dataset version/documents and use separate journals for each explicitly chosen model. Report claims, completed jobs, failures and provider usage separately. Do not launch paid repetitions or a larger campaign from a first-run command; live Gemini evidence and cost measurement remain pending.
