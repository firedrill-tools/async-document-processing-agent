"""Actual CLI/ADK/google-genai over a local HTTP server; no live provider."""
from contextlib import contextmanager
from copy import deepcopy
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
import json
import os
import signal
import subprocess
import sys
from threading import Event,Thread

import pytest

from document_worker.store import JobStore
from test_native import BODY


@contextmanager
def provider(mode):
    requests=[];entered=Event();release=Event()
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*_):pass
        def do_POST(self):
            requests.append(json.loads(self.rfile.read(int(self.headers['Content-Length']))));entered.set()
            if mode=='cancel':release.wait(12);return
            status=503 if mode=='failure' else 200
            body={'error':{'code':503,'message':'Synthetic provider unavailable'}} if mode=='failure' else deepcopy(BODY)
            raw=json.dumps(body).encode();self.send_response(status);self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(raw)));self.send_header('x-request-id','wire-synthetic');self.end_headers();self.wfile.write(raw)
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler);server.daemon_threads=True
    thread=Thread(target=server.serve_forever,daemon=True);thread.start()
    try:yield f'http://127.0.0.1:{server.server_port}',requests,entered
    finally:release.set();server.shutdown();server.server_close();thread.join(timeout=3)


@pytest.mark.parametrize('mode',['success','failure','cancel'])
def test_actual_cli_native_job_completion_failure_and_signal(tmp_path,mode):
    file=tmp_path/'jobs.db';store=JobStore(file);job=store.submit('cli','invoice-wire','Fictional Atlas invoice USD 50.00')
    with provider(mode) as (url,requests,entered):
        program='''
import sys
from document_worker import agent
from document_worker.__main__ import main
original=agent.create_client
async def factory(key,evidence,**options):
 return await original(key,evidence,**{**options,'base_url':sys.argv[1]})
agent.create_client=factory
raise SystemExit(main(sys.argv[2:]))
'''
        env={**os.environ,'GOOGLE_API_KEY':'synthetic-not-real'}
        process=subprocess.Popen([sys.executable,'-c',program,url,'--db',str(file),'work','--once','--model','gemini-2.5-flash'],cwd=tmp_path,env=env,text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        try:
            assert entered.wait(8),'CLI did not reach owned provider'
            if mode=='cancel':process.send_signal(signal.SIGTERM)
            stdout,stderr=process.communicate(timeout=10)
        finally:
            if process.poll() is None:process.kill();process.communicate()
    assert len(requests)==1
    current=store.get(job['job_id']);observation=json.loads(current['attempts'][0]['observation_json'])
    assert observation['native_observations'][0]['input_sha256']
    assert 'synthetic-not-real' not in json.dumps(current)
    if mode=='cancel':
        assert process.returncode==130,stderr
        error=json.loads(stderr.splitlines()[-1]);assert error['worker_receipt']['worker_attempt']==1 and error['worker_receipt']['commit_accepted'] is False
        assert current['status']=='running' and current['result_json'] is None and observation['worker_interrupted'] is True
    else:
        assert process.returncode==(1 if mode=='failure' else 0),stderr
        output=json.loads(stdout);assert output['job_id']==job['job_id'] and output['commit_accepted'] is True
        assert 'document_text' not in output and 'lease_token' not in output and 'lease_token' not in output['attempts'][0]
        assert output['status']==('failed' if mode=='failure' else 'completed')
        if mode=='success':assert output['result']['facts']['total_cents']==5000 and observation['native_observations'][0]['body']==BODY
        else:assert 'facts' not in output['result'] and observation['reported_usage']['fields']['totalTokenCount'] is None
