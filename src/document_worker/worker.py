"""Correlate native observations and facts to one fenced durable attempt."""
from __future__ import annotations

import asyncio
from collections.abc import Awaitable,Callable
from copy import deepcopy
import json

from .agent import ADKExtractor,DocumentFacts,DocumentExtraction,ExtractionError
from .store import JobStore

Processor=Callable[[str],Awaitable[DocumentFacts|DocumentExtraction]]


class DocumentWorker:
    def __init__(self,store:JobStore,processor:Processor,timeout_seconds:float=120):
        if isinstance(timeout_seconds,bool) or not isinstance(timeout_seconds,(int,float)) or not 0<timeout_seconds<=120:
            raise ValueError('worker timeout must be positive and no more than 120 seconds')
        self.store=store;self.processor=processor;self.timeout_seconds=timeout_seconds

    async def run_once(self,lease_seconds:int=300):
        job=self.store.claim(lease_seconds)
        if job is None:return None
        latest=None
        def observe(value):
            nonlocal latest
            latest=deepcopy(value)
            if not self.store.observe(job['job_id'],job['attempt'],job['lease_token'],latest):
                raise RuntimeError('attempt observation rejected; facts cannot commit')
        try:
            try:
                pending=self.processor(job['document_text'],on_observation=observe) if isinstance(self.processor,ADKExtractor) else self.processor(job['document_text'])
                result=await asyncio.wait_for(pending,timeout=self.timeout_seconds)
                if isinstance(result,DocumentExtraction):facts=DocumentFacts.model_validate(result.facts);observe(result.model_observation)
                else:
                    facts=DocumentFacts.model_validate(result)
                    observe({'source':'processor_without_metadata','requested_model':None,'response_models':None,'usage_events':None,'cost_usd':None})
                complete=(facts.document_type in {'invoice','receipt'} and not facts.concerns and all((facts.issuer,facts.currency,facts.total_cents is not None)))
                status='completed' if complete else 'needs_review';failure=None
                payload=json.dumps({'job_id':job['job_id'],'document_id':job['document_id'],'attempt':job['attempt'],
                    'facts':facts.model_dump(),'model_observation':latest,'dataset':json.loads(job['dataset_json']) if job['dataset_json'] else None},allow_nan=False)
            except asyncio.CancelledError as error:
                if getattr(error,'model_observation',None) is not None:latest=deepcopy(error.model_observation)
                interrupted={**(latest or {'source':'processor_without_metadata','cost_usd':None}),
                    'worker_interrupted':True,'provider_outcome':'unknown'}
                self.store.observe(job['job_id'],job['attempt'],job['lease_token'],interrupted,'worker interrupted; provider outcome unknown')
                error.worker_receipt={'job_id':job['job_id'],'document_id':job['document_id'],'worker_attempt':job['attempt'],
                    'commit_accepted':False,'model_observation':interrupted}
                raise
            except Exception as error:
                if isinstance(error,ExtractionError):observe(error.model_observation)
                failure=str(error) or type(error).__name__;status='failed'
                payload=json.dumps({'job_id':job['job_id'],'document_id':job['document_id'],'attempt':job['attempt'],'model_observation':latest},allow_nan=False) if latest is not None else None
            # A finish/storage error has unknown publication outcome. Do not
            # automatically call finish again and overwrite a possible commit.
            committed=self.store.finish(job['job_id'],job['lease_token'],payload,status,error=failure)
            current=self.store.get(job['job_id'])
            return {**current,'worker_attempt':job['attempt'],'commit_accepted':committed,'worker_model_observation':latest} if current is not None else None
        except BaseException as error:
            if not hasattr(error,'worker_receipt'):
                error.worker_receipt={'job_id':job['job_id'],'document_id':job['document_id'],'worker_attempt':job['attempt'],
                    'commit_accepted':None,'publication_outcome':'unknown','model_observation':latest}
            raise

    async def run_forever(self,poll_seconds:float=1):
        if isinstance(poll_seconds,bool) or not isinstance(poll_seconds,(int,float)) or not 0<poll_seconds<=60:raise ValueError('poll_seconds must be positive and no more than 60')
        while True:
            if await self.run_once() is None:await asyncio.sleep(poll_seconds)
