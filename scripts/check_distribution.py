"""Fresh locked dependencies plus wheel and sdist outside the source checkout."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
from tempfile import TemporaryDirectory
import tomllib
from zipfile import ZipFile

ROOT=Path(__file__).resolve().parents[1]
UV=os.environ.get('UV') or shutil.which('uv')
if not UV:raise RuntimeError('uv is required for distribution verification')
VERSION=tomllib.loads((ROOT/'pyproject.toml').read_text())['project']['version']
wheel=ROOT/'dist'/f'async_document_processing_agent-{VERSION}-py3-none-any.whl'
sdist=ROOT/'dist'/f'async_document_processing_agent-{VERSION}.tar.gz'
env={**os.environ}
for key in ('GOOGLE_API_KEY','GEMINI_API_KEY','VIRTUAL_ENV','PYTHONPATH','PYTHONHOME'):env.pop(key,None)


def run(command,cwd,check=True):
    return subprocess.run(command,cwd=cwd,env=env,check=check,text=True,capture_output=True,timeout=180)


def exercise(python,base):
    cli=[str(python),'-I','-m','document_worker']
    run([str(python.parent/'document-worker'),'--help'],base)
    manifest=json.loads(run([*cli,'cases'],base).stdout)
    assert len(manifest['cases'])==8 and manifest['version']==2
    for case in manifest['cases']:
        file=base/f'{case["id"]}.db'
        initialized=json.loads(run([*cli,'--db',str(file),'init-case',case['id']],base).stdout)
        assert initialized['dataset']['case_sha256']==case['sha256'] and file.stat().st_mode&0o777==0o600
    failure=run([*cli,'--db',str(base/'starter.db'),'work','--once','--model','gemini-2.5-flash'],base,False)
    assert failure.returncode==1 and 'GOOGLE_API_KEY' in failure.stderr
    program='''
import asyncio
from pathlib import Path
import sys
import document_worker
from document_worker.agent import DocumentFacts
from document_worker.store import JobStore
from document_worker.worker import DocumentWorker
assert Path(document_worker.__file__).resolve().is_relative_to(Path(sys.prefix).resolve())
base=Path(sys.argv[1]);store=JobStore(base/'busy.db');before=store.list_jobs();assert len(before)==17
async def processor(text):
 assert 'INV-1048' in text
 return DocumentFacts(document_type='invoice',issuer='Northstar Paper Supply',currency='USD',total_cents=5000)
result=asyncio.run(DocumentWorker(store,processor).run_once())
assert result['status']=='completed' and result['commit_accepted'] is True
assert [j for j in before if j['job_id']!=result['job_id']]==[j for j in store.list_jobs() if j['job_id']!=result['job_id']]
assert all(j['attempt']==0 for j in JobStore(base/'starter.db').list_jobs())
assert JobStore(base/'queued-cancelled.db').claim() is None
assert JobStore(base/'interrupted-lease.db').claim()['attempt']==2
assert len(JobStore(base/'duplicate-delivery.db').list_jobs())==1
'''
    run([str(python),'-I','-c',program,str(base)],base)


with TemporaryDirectory(prefix='document-distribution-') as directory:
    temp=Path(directory)
    with ZipFile(wheel) as archive:
        assert 'document_worker/sample_data/manifest.json' in archive.namelist()
        assert len([n for n in archive.namelist() if n.startswith('document_worker/sample_data/') and n.endswith('.json')])==9
    requirements=temp/'requirements.txt'
    requirements.write_text(run([UV,'export','--locked','--no-dev','--no-emit-project','--format','requirements-txt'],ROOT).stdout)
    wheel_env=temp/'wheel-env';run([UV,'venv','--python',sys.executable,str(wheel_env)],temp)
    wheel_python=wheel_env/'bin/python'
    run([UV,'pip','sync','--python',str(wheel_python),str(requirements)],temp)
    run([UV,'pip','install','--python',str(wheel_python),'--no-deps',str(wheel)],temp)
    wheel_case=temp/'wheel-case';wheel_case.mkdir();exercise(wheel_python,wheel_case)
    source_root=temp/'source';source_root.mkdir()
    with tarfile.open(sdist) as archive:archive.extractall(source_root,filter='data')
    source,=source_root.iterdir();assert (source/'uv.lock').exists()
    run([UV,'sync','--locked','--no-dev','--no-editable','--project',str(source)],temp)
    assert (source/'firedrill/scenario.json').exists() and not (source/'firedrill/node_modules').exists()
    run(['npm','ci','--prefix','firedrill','--ignore-scripts','--no-audit','--no-fund'],source)
    run(['npm','test','--prefix','firedrill'],source)
    source_case=temp/'source-case';source_case.mkdir();exercise(source/'.venv/bin/python',source_case)
print('Wheel and sdist passed fresh locked installs, console/module CLI, eight private cases, key preflight, real busy worker, cancellation, duplicate delivery and lease recovery outside checkout.')
