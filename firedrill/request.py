"""One foreground native worker invocation; correlate its receipt to durable state."""
import argparse
import asyncio
import json
from types import SimpleNamespace
from document_worker.__main__ import process_jobs, public_job
from document_worker.store import JobStore

parser = argparse.ArgumentParser(allow_abbrev=False)
parser.add_argument('--db', required=True)
parser.add_argument('--action', choices=['work', 'inspect'], required=True)
parser.add_argument('--model')
args = parser.parse_args()
store = JobStore(args.db)
report = {'executionKind': 'application_state_inspection' if args.action == 'inspect' else 'native_worker_once',
          'before': [public_job(job) for job in store.list_jobs()]}
failed = False
try:
    if args.action == 'work':
        result = asyncio.run(process_jobs(SimpleNamespace(model=args.model, once=True), store))
        report['worker'] = public_job(result) if isinstance(result, dict) else result
        if result is None:
            report['executionOutcome'] = 'no_eligible_job'
        else:
            current = store.get(result['job_id'])
            owned = any(job['job_id'] == result['job_id'] for job in report['before'])
            correlated = owned and current is not None and current['attempt'] == result.get('worker_attempt') and current['status'] == result['status']
            report['receiptMatchesCurrentAttempt'] = correlated
            failed = not (correlated and result.get('commit_accepted') is True and result['status'] in {'completed', 'needs_review'})
            report['executionOutcome'] = result['status'] if not failed else 'unconfirmed_or_failed'
    else:
        report['executionOutcome'] = 'inspected_without_model'
except (Exception, asyncio.CancelledError, KeyboardInterrupt) as error:
    failed = True
    report['error'] = {'message': str(error) or type(error).__name__, 'type': type(error).__name__,
                       'workerReceipt': getattr(error, 'worker_receipt', None) or getattr(error.__context__, 'worker_receipt', None)}
finally:
    report['after'] = [public_job(job) for job in store.list_jobs()]
print(json.dumps(report))
raise SystemExit(1 if failed else 0)
