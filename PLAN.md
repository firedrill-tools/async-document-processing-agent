# Async document worker audit

1. **Native Gemini:** reproduce real ADK/google-genai HTTP completion; reject non-STOP/blocked/multiple/malformed output before facts, bound retries/output/requests/time, and close explicitly owned clients and ephemeral sessions. Keep lower-level injected model ownership clear.
2. **Durable attempts:** preserve raw successful/failed/partial observations before final writes. Fence expired commits even before a replacement claims the job; preserve late attempt evidence without accepting its facts. Correlate original job/document/attempt and cancellation outcomes.
3. **State/data:** validate extracted money and required fields, protect new journals, and supply richer coherent hash-bound starter/busy/ambiguous/boundary cases with model-free private initialization. Prior human processing is distinct from model trials.
4. **Operations/distribution:** exercise normal CLI cancellation/failure/completion, packaged wheel/source data and locked dependencies outside checkout, then exact pushed-head CI and catalog evidence.

Live Gemini judgment/usage, the new official Firedrill contract and publication/license remain parked. Controlled provider responses are boundary proofs, not live output quality or billing.

## Implemented proof

All four task groups are implemented. Sixty-seven offline checks pass, including native Gemini HTTP and loopback CLI success/error/SIGTERM, one-call completion/usage/cleanup, expired/replaced attempt evidence, uncertain commit handling and eight hash-bound cases/four profiles. Fresh locked wheel/sdist gates passed locally. Exact pushed-head Linux CI and catalog/portfolio evidence are the final checkpoints; live/external gates remain parked.
