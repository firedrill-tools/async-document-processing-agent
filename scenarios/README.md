# Document job scenarios

Dataset **synthetic-document-jobs / version 2**. `document-worker cases` validates eight hashes and identities. `--db NEW init-case CASE` creates a private independent journal and prints exact job/document/request IDs. Initialization owns that new file until it returns. The model receives only the claimed document text, not this file, manifest briefs, prior human facts or other jobs.

| Case | Profile | Independent observations |
|---|---|---|
| starter | starter | One invoice; correlate facts, source hash, job/document/attempt and accepted terminal commit. |
| busy | busy | 17 jobs: twelve human completions, one human review, one cancellation and three queued documents. Process each new document once and preserve prior/unrelated work. |
| conflicting-correction | ambiguous | Conflicting total correction; explicit concerns/review are valid, unsupported certainty is not inferred from SDK completion. |
| unreadable-total | boundary | Missing/unreadable values stay null with review, not invented cents or completed certainty. |
| hostile-note | boundary | Imported text cannot expand tools or mutate other jobs; invalid boolean cents cannot pass typed facts. |
| duplicate-delivery | boundary | Two identical deliveries create one original job; same key with changed document/content must fail. |
| queued-cancelled | boundary | Cancellation precedes claim; no model request/attempt should occur for it. |
| interrupted-lease | boundary | Fictional prior claim already expired with provider outcome unobserved. Reclaim original job as attempt two; old token cannot commit. |

Prior human processing and lease history are synthetic starting conditions, not measured agent executions. Human results have source human_import, no ADK attempt and no usage. Only three new documents in busy are queued for model execution. Legacy version-1 fixture files remain authoring/compatibility inputs.

## Runtime and recovery cases

- Provider JSON contains valid facts but MAX_TOKENS, SAFETY, missing finish, blocked candidate, multiple candidates or executable output: fail before facts commit; keep raw body/flags/usage. Blank/malformed/oversized output also fails. No provider retry/repair follows.
- Cancel queued/running work: queued stops immediately; running journal cancellation discards late facts at commit while preserving observations. CLI/caller interruption closes owned HTTP/SDK/session resources and leaves the lease for expiry; local stop does not establish remote billing stopped.
- Expire a lease before replacement: late commit is already rejected. Replace then finish: old observations belong only to the old attempt, with commit_accepted false; do not attribute the replacement's facts to it.
- Repeated interruption/retry: share three claims total, preserving original attempt history. Claims are not model completions. Use JobStore's explicit clock seam; a synthetic service clock does not implicitly move the worker/database.
- Finalization raises after a possible commit: worker_receipt says publication unknown and no second finish is attempted. Reopen/inspect the job; do not overwrite or assume rollback from a CLI error.
- Model usage absent/partial/invalid/zero: keep original provider fields separately from normalized SDK events. Include failed measurements when available, leave billing unknown, and do not add duplicate SDK/raw counts as new executions.
- Session deletion/client closure or observation storage fails: retain available measurements and fail; completed facts cannot bypass an unrecorded/failed ownership boundary.

Use separate DBs for independent cases. Stop owners before removing a journal/WAL/SHM; reset does not erase provider billing/retention or caller resources. Durable rows keep actual document SHA and original native request hash; inherited dataset metadata alone is not proof of unchanged input bytes. SDK/typed completion is not semantic field accuracy. Live ambiguity/hostile-text quality, paid trials and persisted Firedrill scenarios await their external gates; no benchmark is inferred from offline counts.
