import asyncio
import json
from pathlib import Path
import sqlite3

import pytest

from document_worker.agent import ADKExtractor,DocumentFacts
from document_worker.store import JobStore
from document_worker.worker import DocumentWorker
from test_native import BODY,Script
from test_worker import Clock


def test_expired_lease_cannot_commit_before_a_replacement_exists(tmp_path):
    clock=Clock();store=JobStore(tmp_path/'jobs.db',clock=clock);job=store.submit('expired','doc','Fictional invoice')
    claim=store.claim(10);clock.now+=10
    assert store.finish(job['job_id'],claim['lease_token'],'{}') is False
    assert store.get(job['job_id'])['status']=='running' and store.get(job['job_id'])['result_json'] is None
    replacement=store.claim(10)
    assert replacement['attempt']==2 and replacement['attempts'][0]['status']=='lease_expired'


def test_observations_belong_to_original_attempt_after_replacement_and_rejected_commit(tmp_path):
    clock=Clock();store=JobStore(tmp_path/'jobs.db',clock=clock);job=store.submit('late','doc','Fictional invoice')
    old=store.claim(10);clock.now+=11;new=store.claim(10)
    before={'source':'controlled-native','usage':{'total':14}}
    assert store.observe(job['job_id'],old['attempt'],old['lease_token'],before)
    assert store.observe(job['job_id'],new['attempt'],old['lease_token'],{'wrong':True}) is False
    assert store.finish(job['job_id'],new['lease_token'],'{"replacement":true}')
    assert store.finish(job['job_id'],old['lease_token'],'{"wrong":true}') is False
    current=store.get(job['job_id'])
    assert json.loads(current['attempts'][0]['observation_json'])==before and current['attempts'][0]['status']=='lease_expired'
    assert current['result_json']=='{"replacement":true}'


def test_native_raw_observation_is_durable_before_output_and_cancelled_facts_are_discarded(tmp_path,monkeypatch):
    monkeypatch.setenv('GOOGLE_API_KEY','synthetic-not-real')
    store=JobStore(tmp_path/'jobs.db');job=store.submit('cancel','doc','Fictional invoice')
    script=Script([BODY]);original=store.observe;observed=[]
    def cancel_at_body(job_id,attempt,token,observation,error=None):
        result=original(job_id,attempt,token,observation,error)
        native=observation.get('native_observations',[])
        if native and native[-1]['body'] is not None and not observed:
            observed.append(json.loads(store.get(job_id)['attempts'][0]['observation_json']))
            store.cancel(job_id)
        return result
    store.observe=cancel_at_body
    async def run():
        extractor=ADKExtractor('gemini-2.5-flash',transport=script)
        try:return await DocumentWorker(store,extractor).run_once()
        finally:await extractor.close()
    result=asyncio.run(run())
    assert observed[0]['native_observations'][0]['body']==BODY
    assert result['status']=='cancelled' and result['result_json'] is None and script.closed
    history=result['attempts'][0]
    assert 'facts' not in json.loads(history['result_json'])
    assert json.loads(history['observation_json'])['reported_usage']['fields']['totalTokenCount']==14


def test_native_worker_cancellation_preserves_running_attempt_before_lease_recovery(tmp_path,monkeypatch):
    import httpx
    monkeypatch.setenv('GOOGLE_API_KEY','synthetic-not-real')
    clock=Clock();store=JobStore(tmp_path/'jobs.db',clock=clock);job=store.submit('interrupt','doc','Fictional invoice')
    async def run():
        entered=asyncio.Event();closed=[]
        class Pending(httpx.AsyncBaseTransport):
            async def handle_async_request(self,request):entered.set();await asyncio.Event().wait()
            async def aclose(self):closed.append(True)
        extractor=ADKExtractor('gemini-2.5-flash',transport=Pending());task=asyncio.create_task(DocumentWorker(store,extractor).run_once(lease_seconds=10))
        try:
            await entered.wait();task.cancel()
            with pytest.raises(asyncio.CancelledError) as error:await task
            assert error.value.worker_receipt['commit_accepted'] is False and closed
            current=store.get(job['job_id']);observation=json.loads(current['attempts'][0]['observation_json'])
            assert current['status']=='running' and observation['worker_interrupted'] is True
            assert observation['native_observations'][0]['status']=='failed' and observation['reported_usage']['fields']['totalTokenCount'] is None
        finally:await extractor.close()
    asyncio.run(run());clock.now+=11
    assert store.claim()['attempts'][0]['status']=='lease_expired'


def test_finish_error_after_commit_is_not_retried_as_failure(tmp_path):
    store=JobStore(tmp_path/'jobs.db');job=store.submit('uncertain','doc','Fictional invoice');original=store.finish;calls=[]
    def uncertain(*args,**options):
        calls.append(True);original(*args,**options);raise OSError('synthetic error after journal commit')
    store.finish=uncertain
    async def process(_):return DocumentFacts(document_type='invoice',issuer='Synthetic Atlas',currency='USD',total_cents=5000)
    with pytest.raises(OSError) as error:asyncio.run(DocumentWorker(store,process).run_once())
    assert len(calls)==1 and store.get(job['job_id'])['status']=='completed'
    assert error.value.worker_receipt['publication_outcome']=='unknown' and error.value.worker_receipt['commit_accepted'] is None


def test_new_journal_is_private_and_legacy_history_is_migrated_without_fabricated_evidence(tmp_path):
    file=tmp_path/'jobs.db';store=JobStore(file);assert file.stat().st_mode&0o777==0o600
    job=store.submit('legacy','doc','Fictional invoice');claim=store.claim()
    with sqlite3.connect(file) as db:
        db.execute('ALTER TABLE job_attempts DROP COLUMN observation_json')
        db.execute('ALTER TABLE job_attempts DROP COLUMN lease_token')
    migrated=JobStore(file);history=migrated.get(job['job_id'])['attempts'][0]
    assert history['lease_token'] is None and history['observation_json'] is None
    assert migrated.observe(job['job_id'],1,claim['lease_token'],{'source':'cannot-reconstruct'}) is False
    assert migrated.finish(job['job_id'],claim['lease_token'],'{}') is True
