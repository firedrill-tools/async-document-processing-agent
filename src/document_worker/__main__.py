"""Command-line interface for durable document jobs."""

import argparse
import asyncio
import json
from pathlib import Path

from .agent import ADKExtractor
from .store import JobStore
from .worker import DocumentWorker


def public_job(job: dict | None) -> dict | None:
    if job is None:
        return None
    visible = {key: value for key, value in job.items() if key not in {
        "document_text", "content_sha256", "lease_token"
    }}
    if visible.get("result_json"):
        visible["result"] = json.loads(visible.pop("result_json"))
    else:
        visible.pop("result_json", None)
    return visible


def main() -> None:
    parser = argparse.ArgumentParser(description="Durable Google ADK document worker")
    parser.add_argument("--db", default="jobs.db", help="SQLite job journal path")
    commands = parser.add_subparsers(dest="command", required=True)
    submit = commands.add_parser("submit")
    submit.add_argument("--key", required=True, help="idempotency key")
    submit.add_argument("--document-id", required=True)
    submit.add_argument("--file", type=Path, required=True)
    for name in ("get", "cancel", "retry"):
        commands.add_parser(name).add_argument("job_id")
    commands.add_parser("list")
    work = commands.add_parser("work")
    work.add_argument("--once", action="store_true")
    work.add_argument("--model", help="Gemini model enabled for this account")
    args = parser.parse_args()
    store = JobStore(args.db)
    if args.command == "submit":
        output = store.submit(args.key, args.document_id, args.file.read_text())
    elif args.command == "get":
        output = store.get(args.job_id)
    elif args.command == "cancel":
        output = store.cancel(args.job_id)
    elif args.command == "retry":
        output = store.retry(args.job_id)
    elif args.command == "list":
        output = store.list_jobs()
    else:
        async def process_jobs():
            extractor = ADKExtractor(model=args.model)
            try:
                worker = DocumentWorker(store, extractor)
                return await (worker.run_once() if args.once else worker.run_forever())
            finally:
                await extractor.close()
        output = asyncio.run(process_jobs())
    if isinstance(output, list):
        output = [public_job(job) for job in output]
    elif isinstance(output, dict):
        output = public_job(output)
    print(json.dumps(output, indent=2))


if __name__ == "__main__":
    main()
