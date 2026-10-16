"""Noninteractive job CLI; owned worker signals preserve the durable journal."""
import argparse
import asyncio
import json
from pathlib import Path
import signal
import sys

from .datasets import initialize,list_cases
from .agent import ADKExtractor
from .store import JobStore
from .worker import DocumentWorker


def public_job(job):
    if job is None:return None
    visible={key:value for key,value in job.items() if key not in {'document_text','content_sha256','lease_token'}}
    if visible.get('result_json'):visible['result']=json.loads(visible.pop('result_json'))
    else:visible.pop('result_json',None)
    if visible.get('observation_json'):visible['model_observation']=json.loads(visible.pop('observation_json'))
    else:visible.pop('observation_json',None)
    if visible.get('dataset_json'):visible['dataset']=json.loads(visible.pop('dataset_json'))
    else:visible.pop('dataset_json',None)
    if 'attempts' in visible:visible['attempts']=[public_job(attempt) for attempt in visible['attempts']]
    return visible


async def process_jobs(args,store):
    loop=asyncio.get_running_loop();task=asyncio.current_task();previous=signal.getsignal(signal.SIGTERM)
    loop.add_signal_handler(signal.SIGTERM,task.cancel)
    extractor=None
    try:
        extractor=ADKExtractor(model=args.model)
        worker=DocumentWorker(store,extractor)
        return await (worker.run_once() if args.once else worker.run_forever())
    finally:
        try:
            if extractor is not None:await extractor.close()
        finally:loop.remove_signal_handler(signal.SIGTERM);signal.signal(signal.SIGTERM,previous)


def main(argv=None):
    parser=argparse.ArgumentParser(description='Durable Google ADK document worker',allow_abbrev=False)
    parser.add_argument('--db',default='jobs.db',help='SQLite job journal path')
    commands=parser.add_subparsers(dest='command',required=True)
    submit=commands.add_parser('submit',allow_abbrev=False)
    submit.add_argument('--key',required=True,help='idempotency key')
    submit.add_argument('--document-id',required=True)
    submit.add_argument('--file',type=Path,required=True)
    for name in ('get','cancel','retry'):commands.add_parser(name).add_argument('job_id')
    commands.add_parser('list')
    commands.add_parser('cases')
    commands.add_parser('init-case').add_argument('case_id')
    work=commands.add_parser('work',allow_abbrev=False)
    work.add_argument('--once',action='store_true');work.add_argument('--model',help='Gemini Developer API model enabled for this account')
    args=parser.parse_args(argv)
    try:
        if args.command in {'cases','init-case'}:
            output=list_cases() if args.command=='cases' else initialize(args.case_id,Path(args.db))
            print(json.dumps(output,indent=2));return 0
        store=JobStore(args.db)
        if args.command=='submit':output=store.submit(args.key,args.document_id,args.file.read_bytes().decode('utf8'))
        elif args.command=='get':output=store.get(args.job_id)
        elif args.command=='cancel':output=store.cancel(args.job_id)
        elif args.command=='retry':output=store.retry(args.job_id)
        elif args.command=='list':output=store.list_jobs()
        else:output=asyncio.run(process_jobs(args,store))
        visible=[public_job(job) for job in output] if isinstance(output,list) else public_job(output) if isinstance(output,dict) else output
        print(json.dumps(visible,indent=2))
        if args.command=='work' and isinstance(output,dict) and (output.get('commit_accepted') is not True or output.get('status') not in {'completed','needs_review'}):return 1
        return 0
    except (Exception,asyncio.CancelledError,KeyboardInterrupt) as error:
        receipt=getattr(error,'worker_receipt',None) or getattr(error.__context__,'worker_receipt',None)
        print(json.dumps({'error':str(error) or type(error).__name__,'type':type(error).__name__,'worker_receipt':receipt}),file=sys.stderr)
        return 130 if isinstance(error,(asyncio.CancelledError,KeyboardInterrupt)) else 1


if __name__=='__main__':raise SystemExit(main())
