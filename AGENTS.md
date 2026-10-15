# Working rules

Keep this a standalone, synthetic document-job application. Do not put provider keys or real documents in fixtures or tests. Keep Google ADK's invocation behind `ADKExtractor` so the job journal can be tested without a live provider.

Preserve job IDs, idempotency and fenced lease semantics across changes. A cancelled or superseded worker must not commit a result. Do not report a stub-model test as a live Gemini proof. Add Firedrill integration only against the verified new release, never the retired client.
