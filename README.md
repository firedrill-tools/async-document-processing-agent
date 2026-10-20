# Async document processing agent with Google ADK and Gemini

Submit a fictional invoice or receipt, let a worker extract typed facts later, and inspect the exact correlated job/attempt. Google ADK runs Gemini; SQLite owns idempotency, leases, cancellation, recovery and committed records. Earlier observations survive failed or rejected completion. This application accepts text, not PDF/OCR input, and has no external business-write tools.

Simulate this Google ADK AI agent with [Firedrill](https://firedrill.run/), a simulation platform for AI agents. Start with the [Firedrill setup and simulation guide](firedrill/README.md); your coding agent can operate it while this application's runner stays in your repository or CI.

This example repository is publicly viewable. A project license has not been selected.

## Run one case

Requires Python 3.12+ and `uv`. Listing, initialization, submission and journal inspection need no key.

```sh
uv sync --locked --extra test
uv run --locked document-worker cases
uv run --locked document-worker --db run-output/jobs.db init-case busy
uv run --locked document-worker --db run-output/jobs.db list
uv run --locked document-worker --db run-output/jobs.db work --once --model gemini-2.5-flash
```

Set `GOOGLE_API_KEY` through your environment/secret manager before `work`. `--model` or `GEMINI_MODEL` must name an enabled `gemini-*` Developer API model. Normal execution uses an explicit API key, `vertexai=False` and `https://generativelanguage.googleapis.com/v1beta`, without environment endpoint/backend fallback. Fresh model requests incur provider usage. The normal CLI has no scripted production mode. `python -m document_worker` is the same entry point.

`init-case` validates bundled hashes/identity and exclusively creates a private database; it never overwrites. It prints dataset hashes and exact job/document/request IDs. Use separate files per independent case and wait for initialization to finish before starting its workers. For custom inputs:

```sh
uv run --locked document-worker --db run-output/custom.db submit \
  --key delivery-1048 --document-id invoice-a --file fixtures/invoice-a.txt
uv run --locked document-worker --db run-output/custom.db get JOB_ID
```

`cancel JOB_ID`, `retry JOB_ID`, `get JOB_ID` and `list` use that same `--db`. `work` polls continuously; `--once` claims at most one job and returns `null` when none is eligible. Both API and CLI distinguish committed terminal state from an attempt that returned late. A failed/cancelled/rejected `work --once` exits nonzero; SIGINT/SIGTERM exit 130. Other CLI errors are JSON on stderr. The CLI omits source text and lease-token fields from job/attempt records; raw observations/results remain inspectable.

The callable seam uses the same journal and runtime:

```python
from pathlib import Path
from document_worker.datasets import initialize
from document_worker.store import JobStore
from document_worker.agent import ADKExtractor
from document_worker.worker import DocumentWorker

initialize("starter", Path("run-output/one.db"))
extractor = ADKExtractor("gemini-2.5-flash")
try:
    result = await DocumentWorker(JobStore("run-output/one.db"), extractor).run_once()
finally:
    await extractor.close()
```

## Native completion and ownership

Execution pins Google ADK **2.12.0**, google-genai **2.31.0** and HTTPX **0.28.1**. A native HTTP probe found ADK accepting valid facts with `MAX_TOKENS`, `SAFETY` and no finish flag. Public HTTP hooks now require one unblocked `STOP` candidate with nonempty final text before SDK normalization. Executable/nontext content, malformed/oversized bodies and inconsistent roles fail. Typed facts reject coerced/unsafe cents, malformed dates/currencies, blank concerns and unknown fields. Missing issuer/currency/total, unknown document type or a reported concern produces `needs_review`.

Each normal extraction owns a fresh Gemini client, HTTP client, model, ADK runner and unique ephemeral session. All are closed/deleted after success, startup failure, invalid output, provider failure, cancellation or deadline. Google SDK deliberately leaves an injected HTTP client open; this application closes its owned client separately. `close()` also cancels an active owned extraction and rejects new calls. An extractor accepts one active document; use independent instances for concurrency. Injected `BaseLlm` models/clients remain caller-owned; their scripted runner tests are separate from native transport qualification.

Normal limits are **one model call**, 4,096 output tokens, one candidate, 30-second HTTP phase timeouts and a 120-second extraction/worker deadline. SDK attempts are one and ADK's call budget is one. There is no automatic provider repair/retry. JSON `generateContent` is qualified; wire SSE is not. `create_client` exposes explicit transport/endpoint composition while retaining the native SDK. Local cancellation closes resources but does not establish that remote generation or billing stopped.

## Durable state and completion

| Operation | Committed behavior |
|---|---|
| Submit | Exact request key + document ID + UTF-8 content hash; same delivery returns original job, changed delivery fails |
| Claim | One SQLite transaction chooses a job and issues an attempt/lease token |
| Observe | Original attempt/token owns its observations, including evidence arriving after replacement |
| Finish | Only the current **unexpired** running lease can commit facts/status |
| Cancel | Queued job stops immediately; running job marks cancellation and discards late facts |
| Retry/recover | Same job, fresh attempt/token; previous evidence retained, maximum three claims total |

Lease expiry fences a result even before another worker reclaims it. A replacement can finish independently while the old attempt retains its own observations. `worker_attempt`, `worker_model_observation` and `commit_accepted` identify the returning worker; its top-level job may show a replacement's current state. Never attribute that replacement's facts to a rejected old commit.

Cancellation through the journal is cooperative at commit time. Caller cancellation or CLI signals stop owned local work, preserve available observations, and leave the running lease for recovery; abrupt termination also waits for expiry. An expired/cancelled attempt does not prove provider cost zero. Three explicit retries/expired-lease recoveries share one claim budget; claim count is not completed model count. `retry` accepts only failed jobs and clears the current result while retaining history. No synthetic platform clock is assumed to advance the application's real clock; `JobStore(clock=...)` is the explicit cooperative clock seam.

SQLite transactions protect concurrent claims and completion. New journals are mode 0600 under newly created private directories; existing journals retain their owner's permissions. Connections close after each operation and no transaction spans model I/O. The database stores source text, hashes, dataset metadata, results and attempt history. A finalization/storage error has unknown publication outcome: the worker raises with `worker_receipt`, does not blindly finalize again, and requires inspection of the current job. No rollback of a possibly committed result is claimed.

A fresh inactive database resets application state. Stop its workers before deleting that database and its WAL/SHM sidecars. Reset does not erase provider retention/billing or caller-owned resources. No persistent ADK conversation or provider interaction chain is configured; ephemeral session cleanup does not delete the durable journal.

## Observations and usage

`get` includes every attempt's result/error, timestamps and separately persisted model observation. Native request/response observations are written before facts can commit, preserving decoded HTTP body/error text, original finish/model/response/request IDs, request/body hashes, generation limits and actual installed versions. Raw captures precede SDK defaults. The 1 MiB evidence cap applies after public HTTP buffering, not on the network or to memory. A read failure may leave body/usage unavailable; missing data stays unknown. Observation write failures prevent fact commitment.

Raw `reported_usage` distinguishes absent/partial/invalid and explicit zero counts from SDK event metadata. Provider prompt/candidate/total counters are retained as reported, with no guessed total from incomplete fields and no inferred billing. Failed attempts keep reported measurements. `usage_events` are distinct nonpartial ADK SDK events; do not sum those and the raw body counters as separate executions. Cost stays null. A stopped model turn is not a field-accuracy score.

Superseded attempts retain evidence without accepting their facts. A cancelled result can keep model usage while its facts are discarded. Existing journals migrate history/dataset columns, but absent historical observations/lease ownership cannot be reconstructed or backfilled. Human-imported records are explicitly `human_import`, zero model claims and no usage. Forced termination can leave a running attempt with partial observations.

## Data and checks

Dataset **synthetic-document-jobs / version 2** has eight hash-bound cases/four profiles: starter, busy, conflicting correction, unreadable total, hostile note, duplicate delivery, queued cancellation and interrupted lease. Busy contains **17 jobs**: twelve coherent prior human completions, one human review, one cancellation and three queued documents. Prior worker leases are fictional lifecycle history with no claim that a provider request occurred. No model decisions/usage are seeded. Only the submitted text enters Gemini; dataset/assessment briefs and other jobs remain outside its context. Initializer records manifest/case hashes; the journal's document hash and native request hash identify actual inputs after later edits.

```sh
uv run --locked --extra test pytest -q
uv run --locked python scripts/build_fixtures.py
uv build
uv run --locked python scripts/check_distribution.py
```

The 67 offline checks include actual ADK/Gemini/native HTTP, loopback CLI completion/error/SIGTERM, raw usage and cleanup, expired/replaced attempts, uncertain commits, SQLite concurrency/recovery and dataset drift. Separate `BaseLlm` stub tests exercise framework state. Controlled responses verify boundaries, not Gemini judgment or a success rate. The legacy five text fixtures/version-1 manifest remain authoring/compatibility inputs. [Scenario briefs](scenarios/README.md) describe independent observations.

Wheel and sdist gates install fresh locked dependencies outside checkout and exercise console/module CLI, eight private cases, key preflight, real busy-worker state changes, duplicate delivery, cancellation and expired-lease recovery. CI also regenerates exact case bytes. Live Gemini field quality/budget, authenticated Firedrill persistence verification, and license choice remain parked. Use the existing CLI/callable/journal/observation seams for a thin adapter only after the official release is qualified. Explore the [example catalog](https://github.com/firedrill-tools/firedrill-example-catalog), [Firedrill](https://firedrill.run/) and [official documentation](https://docs.firedrill.run/).

## Current Firedrill integration

Released CLI/SDK 2.2.2, current command invocation, remote operator MCP and recovery are documented in [firedrill/README.md](firedrill/README.md). Offline checks exercise actual durable state and native key preflight; authenticated persisted runs and live model quality/cost remain separate gates.
