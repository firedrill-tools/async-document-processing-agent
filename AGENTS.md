# Working rules

Keep this a standalone, synthetic document-job application. Do not put provider keys or real documents in fixtures or tests. Keep Google ADK's invocation behind `ADKExtractor` so the job journal can be tested without a live provider.

Preserve job IDs, idempotency and fenced lease semantics across changes. A cancelled or superseded worker must not commit a result. Do not report a stub-model test as a live Gemini proof. Add Firedrill integration only against the verified new release, never the retired client.

`src/document_worker/__main__.py` is the CLI; `worker.py` owns processing/completion, `store.py` owns transactional state, and `agent.py` owns the ADK model call. `--db` selects a journal; independent cases use separate files. `fixtures/manifest.json` pins synthetic dataset version 1 and its four profiles; `scenarios/README.md` describes invocation, completion, reset, cancellation and observation-only exploration. The target receives only submitted document text.

Run `uv sync --locked --extra test && uv run --locked --extra test pytest -q` for the 19 offline checks. Worker claims, including recovery after expired leases, share a hard budget of three. A lease duration must be a positive integer. Preserve review status for missing fields, unknown types or reported concerns. Delete each ephemeral ADK session in `finally`, and close an owned extractor/runner after use. Journal data deliberately remains until its owner removes an inactive database and sidecars. Missing token usage stays unknown; a claim is not proof of a completed model execution.
