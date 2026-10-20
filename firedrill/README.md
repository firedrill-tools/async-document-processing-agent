# Firedrill agent-first integration

The optional toolchain pins released `@firedrill-run/cli` and `@firedrill-run/sdk` **2.2.2** (October 19, 2026), Node 22+. The app retains its locked Google ADK, GenAI and HTTPX dependencies.

## Set up Firedrill

From the repository root, install this application's locked dependencies and the optional simulation clients:

```sh
uv sync --locked --extra test
npm ci --prefix firedrill --ignore-scripts --no-audit --no-fund
```

Sign in and choose the Firedrill project and environment once:

```sh
npx firedrill-run init
```

Complete browser authorization when prompted. Initialization writes `firedrill.config.json` in this repository; it does not run the AI agent. Installation choices are optional: skip duplicate SDK/client installation if you are using the locked `firedrill/` toolchain above. Keep this application's runtime requirements from the root README; the clients' Node.js minimum does not replace them.

Automation uses protected `FIREDRILL_TOKEN`, `FIREDRILL_PROJECT_ID`, `FIREDRILL_ENVIRONMENT_ID`. Model operations independently need `GOOGLE_API_KEY` and an enabled `GEMINI_MODEL` or recipe model parameter. Do not put credentials in recipes.

After scope and provider budget are authorized:

```sh
node firedrill/simulate.mjs --preview
node firedrill/simulate.mjs
```

The sequential recipe runs the real foreground `DocumentWorker` through the application’s existing `process_jobs` lifecycle, native ADK extractor and fenced SQLite store. Starter and conflicting-correction cases request one actual model extraction; busy/cancelled inspection cases read actual state with no model call. Every attempt initializes its own hash-validated private DB; no production fallback, injected processor, background daemon, implicit retry or automatic finalization is added.

The target first retains `.runs/firedrill/attempt-*/invocation.json` with validated outer run/attempt/scenario identity. Explicit input selects reviewed case and `work`/`inspect`; only model may be configured. Controller/assessment payloads and unsupported Tool bindings are rejected. Operator/CI authority is removed before application dispatch. Only the document text enters the model; fixture expectations and outer invocation metadata stay outside that input.

Worker output preserves its exact job ID, worker attempt, observations and `commit_accepted`, then compares those to the actual current stored attempt. Success requires accepted commitment to that same attempt and completed/needs_review state. A null receipt is explicitly `no_eligible_job`, not a newly completed extraction. Inspection is labeled `inspected_without_model`. Exceptions preserve the original worker receipt and after-state; unknown publication remains unknown. Existing native owned-session/client cleanup, leases, cancellation and claim limits remain in effect. New invocations create new DBs; they do not recover an earlier app lease or erase earlier evidence. Preserve earlier DB/WAL/SHM and reports for inspection. Local cleanup does not guarantee remote cancellation/billing or rollback. Local paths are not uploaded Firedrill artifacts; field accuracy is a separate assessment.

## Offline verification

These checks are optional when getting started; they do not authorize an account or run a paid model:

```sh
node firedrill/node_modules/@firedrill-run/cli/bin/firedrill.mjs validate firedrill/scenario.json --json --non-interactive
npm test --prefix firedrill
```

The saved recipe is an observation/exploration example, not a claim of agent correctness. It has no Firedrill rubric or CI gate. Execution success and any application-owned checks are distinct from a scored assessment. Local evidence paths are not uploaded captures; the evidence and proof limits described in this guide remain in effect.

## MCP and recovery

Configure remote OAuth operator MCP at **https://mcp.firedrill.run/mcp** using [host setup](https://docs.firedrill.run/guides/coding-agents). MCP prepares, queues and inspects; the local SDK/CLI executes the command target. Host OAuth and execution login are separate. Use `--run BATCH_ID` to attach queued work, `--resume OPERATION_ID` after uncertainty, and `--result BATCH_ID` to read retained results. Preserve SDK journals and local evidence. These scripts retain public SDK diagnostics and whole-batch exit status; a new simulation starts new work.

CI and the fresh locked source archive run offline integration tests with real state inspection and the native missing-key boundary. They establish wiring and cleanup, not live model quality, platform persistence, or uploaded captures. The observation recipe defines no grader/gate. Authenticated persisted happy/negative runs, paid model quality/cost, publication and licensing remain separate gates.

References: [SDK](https://docs.firedrill.run/sdk/typescript), [command targets](https://docs.firedrill.run/guides/callbacks), [operator MCP](https://docs.firedrill.run/guides/control-mcp), [CI](https://docs.firedrill.run/guides/ci).
