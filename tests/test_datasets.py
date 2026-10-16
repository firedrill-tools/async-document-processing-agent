from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import asyncio

import pytest

from document_worker.datasets import data_root,initialize,list_cases
from document_worker.agent import ADKExtractor
from document_worker.store import JobStore
from document_worker.worker import DocumentWorker
from test_native import Script,BODY


def test_eight_hash_bound_private_cases_are_independent_and_never_overwrite(tmp_path):
    manifest=list_cases();assert manifest['version']==2 and len(manifest['cases'])==8
    assert {c['profile'] for c in manifest['cases']}=={'starter','busy','ambiguous','boundary'}
    for case in manifest['cases']:
        file=tmp_path/f'{case["id"]}.db';selected=initialize(case['id'],file)
        assert file.stat().st_mode&0o777==0o600 and selected['dataset']['case_sha256']==case['sha256']
        assert selected['dataset']['manifest_sha256']==manifest['manifest_sha256']
        before=JobStore(file).list_jobs()
        with pytest.raises(FileExistsError):initialize(case['id'],file)
        assert JobStore(file).list_jobs()==before
    assert len(JobStore(tmp_path/'duplicate-delivery.db').list_jobs())==1
    assert JobStore(tmp_path/'queued-cancelled.db').claim() is None
    recovered=JobStore(tmp_path/'interrupted-lease.db').claim()
    assert recovered['attempt']==2 and recovered['attempts'][0]['status']=='lease_expired' and recovered['attempts'][0]['observation_json'] is None


def test_busy_human_history_is_distinct_and_native_job_preserves_other_records(tmp_path,monkeypatch):
    monkeypatch.setenv('GOOGLE_API_KEY','synthetic-not-real')
    file=tmp_path/'busy.db';selected=initialize('busy',file);store=JobStore(file);before=store.list_jobs()
    assert len(before)==17 and sum(j['status']=='completed' for j in before)==12 and sum(j['status']=='queued' for j in before)==3
    for job in before:
        assert job['attempt']==0
        if job['status']=='completed':assert json.loads(job['result_json'])['model_observation']['source']=='human_import'
    target=next(j for j in before if j['status']=='queued');script=Script([BODY])
    async def run():
        extractor=ADKExtractor('gemini-2.5-flash',transport=script)
        try:return await DocumentWorker(store,extractor).run_once()
        finally:await extractor.close()
    result=asyncio.run(run())
    assert result['job_id']==target['job_id'] and result['status']=='completed' and script.closed and len(script.requests)==1
    assert [j for j in store.list_jobs() if j['job_id']!=target['job_id']]==[j for j in before if j['job_id']!=target['job_id']]
    payload=json.loads(result['result_json']);assert payload['dataset']==selected['dataset']
    text=json.dumps(script.requests[0]['body']['contents'])
    assert 'INV-1048' in text and 'HUM-001' not in text and 'Human reviewer' not in text and 'case_sha256' not in text


@pytest.mark.parametrize('mode',['hash','identity','path','duplicate','seeded-model','conflict'])
def test_dataset_drift_and_fabricated_seeded_results_fail_before_journal_creation(tmp_path,mode):
    root=tmp_path/'data';shutil.copytree(data_root(),root);manifest_file=root/'manifest.json';manifest=json.loads(manifest_file.read_text());case=manifest['cases'][0];target=root/case['file']
    if mode=='hash':target.write_bytes(target.read_bytes()+b'\n')
    elif mode=='path':case['file']='../escape.json'
    elif mode=='duplicate':manifest['cases'][1]['id']=case['id']
    else:
        state=json.loads(target.read_text())
        if mode=='identity':state['dataset']['case_id']='wrong'
        elif mode=='seeded-model':state['records'][0]['facts']={'total_cents':5000}
        else:state['records'].append({**state['records'][0],'text':'Different content with same delivery identity'})
        raw=json.dumps(state).encode();target.write_bytes(raw);case['sha256']=hashlib.sha256(raw).hexdigest()
    manifest_file.write_text(json.dumps(manifest));file=tmp_path/'new.db'
    with pytest.raises(ValueError):initialize('starter',file,root)
    assert not file.exists()


def test_actual_cli_model_free_cases_init_and_key_preflight(tmp_path):
    env={**os.environ};env.pop('GOOGLE_API_KEY',None);env.pop('GEMINI_API_KEY',None)
    command=[sys.executable,'-m','document_worker']
    def run(*args):return subprocess.run([*command,*args],cwd=tmp_path,env=env,text=True,capture_output=True,timeout=10)
    listed=run('cases');assert listed.returncode==0 and len(json.loads(listed.stdout)['cases'])==8 and not (tmp_path/'jobs.db').exists()
    file=tmp_path/'jobs.db';selected=run('--db',str(file),'init-case','busy');assert selected.returncode==0 and len(json.loads(selected.stdout)['jobs'])==17
    before=JobStore(file).list_jobs();assert run('--db',str(file),'init-case','starter').returncode==1 and JobStore(file).list_jobs()==before
    failed=run('--db',str(file),'work','--once','--model','gemini-2.5-flash');assert failed.returncode==1 and 'GOOGLE_API_KEY' in failed.stderr
    assert JobStore(file).list_jobs()==before
    output=json.loads(run('--db',str(file),'list').stdout);assert len(output)==17 and 'document_text' not in output[0] and 'lease_token' not in output[0]
