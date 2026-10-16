"""Hash-bound independent SQLite cases; target receives only document text."""
import hashlib
import json
import os
from pathlib import Path
import time

from .agent import DocumentFacts
from .store import JobStore


def data_root():
    bundled=Path(__file__).parent/'sample_data'
    return bundled if bundled.exists() else Path(__file__).parents[2]/'data'


def load_manifest(root=None):
    root=Path(root) if root is not None else data_root();raw=(root/'manifest.json').read_bytes();manifest=json.loads(raw)
    if (manifest.get('id'),manifest.get('version'),manifest.get('synthetic'))!=('synthetic-document-jobs',2,True):raise ValueError('unsupported dataset identity')
    cases=manifest.get('cases')
    if not isinstance(cases,list) or not cases:raise ValueError('empty cases')
    ids=set();files=set()
    for case in cases:
        if not isinstance(case,dict) or not isinstance(case.get('id'),str) or not case['id'] or case['id'] in ids:raise ValueError('duplicate/invalid case identity')
        ids.add(case['id']);file=case.get('file')
        if not isinstance(file,str) or not file or Path(file).is_absolute() or '..' in Path(file).parts or file in files:raise ValueError('invalid case path')
        files.add(file)
        if case.get('profile') not in {'starter','busy','ambiguous','boundary'}:raise ValueError('invalid profile')
        target=(root/file).resolve(strict=True)
        if not target.is_relative_to(root.resolve(strict=True)):raise ValueError('case escapes dataset')
        content=target.read_bytes()
        if hashlib.sha256(content).hexdigest()!=case.get('sha256'):raise ValueError('case hash differs')
        validate_case(json.loads(content),case,manifest)
    return manifest,hashlib.sha256(raw).hexdigest(),root


def validate_case(state,case,manifest):
    if state.get('dataset')!={'id':manifest['id'],'version':2,'profile':case['profile'],'case_id':case['id']} or state.get('synthetic') is not True:raise ValueError('case identity differs')
    records=state.get('records')
    if not isinstance(records,list) or not records or len(records)>1000:raise ValueError('invalid case records')
    seen={};documents={};expired=0
    for record in records:
        if not isinstance(record,dict) or not all(isinstance(record.get(key),str) and record[key].strip() for key in ('key','document_id','text')):raise ValueError('invalid document record')
        if len(record['key'])>200 or len(record['document_id'])>200 or len(record['text'])>100000:raise ValueError('oversized document input')
        if record.get('state') not in {'queued','cancelled','human_completed','human_review','expired_lease'}:raise ValueError('invalid initial job state')
        if record['key'] in seen and (seen[record['key']]!=record or record['state']!='queued'):raise ValueError('conflicting repeated delivery')
        seen[record['key']]=record
        if record['document_id'] in documents and documents[record['document_id']]!=record['text']:raise ValueError('conflicting document identity')
        documents[record['document_id']]=record['text']
        if record['state'].startswith('human_'):
            facts=DocumentFacts.model_validate(record.get('facts'))
            complete=facts.document_type in {'invoice','receipt'} and not facts.concerns and all((facts.issuer,facts.currency,facts.total_cents is not None))
            if complete!=(record['state']=='human_completed'):raise ValueError('human facts and state disagree')
        elif 'facts' in record:raise ValueError('queued documents cannot contain seeded facts')
        expired+=record['state']=='expired_lease'
    if expired and (expired!=1 or len(records)!=1):raise ValueError('expired-lease fixture must isolate one original claim')


def list_cases(root=None):
    manifest,sha,_=load_manifest(root);return {**manifest,'manifest_sha256':sha}


def initialize(case_id,path,root=None):
    manifest,manifest_sha,root=load_manifest(root);case=next((c for c in manifest['cases'] if c['id']==case_id),None)
    if case is None:raise ValueError('unknown case')
    raw=(root/case['file']).read_bytes()
    if hashlib.sha256(raw).hexdigest()!=case['sha256']:raise ValueError('case changed before initialization')
    state=json.loads(raw);validate_case(state,case,manifest)
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
    descriptor=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600);os.close(descriptor)
    identity={**state['dataset'],'case_sha256':case['sha256'],'manifest_sha256':manifest_sha}
    # A prior claim is explicitly fictional lifecycle history and already expired
    # under the real application clock; it does not imply a provider call occurred.
    clock_value=time.time()-600
    def clock():return clock_value
    try:
        store=JobStore(path,clock=clock);jobs={}
        for record in state['records']:
            clock_value+=1
            row=store.submit(record['key'],record['document_id'],record['text'],identity)
            if record['state'].startswith('human_'):row=store.import_human(row['job_id'],DocumentFacts.model_validate(record['facts']).model_dump(),'completed' if record['state']=='human_completed' else 'needs_review')
            elif record['state']=='cancelled':row=store.cancel(row['job_id'])
            elif record['state']=='expired_lease':row=store.claim(lease_seconds=5)
            jobs[row['job_id']]={'job_id':row['job_id'],'document_id':row['document_id'],'request_key':row['request_key'],'status':row['status'],'attempt':row['attempt']}
        return {'db':str(path.resolve()),'dataset':identity,'jobs':list(jobs.values()),'deliveries':len(state['records'])}
    except BaseException:
        for file in (path,Path(str(path)+'-wal'),Path(str(path)+'-shm')):file.unlink(missing_ok=True)
        raise
