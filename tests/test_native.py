"""Actual ADK/Gemini/google-genai path over controlled HTTP, no model stub."""
import asyncio
from copy import deepcopy
import json
from pathlib import Path

import httpx
import pytest

from document_worker import agent as agent_module
from document_worker.agent import ADKExtractor,ExtractionError
from document_worker.store import JobStore
from document_worker.worker import DocumentWorker

FACTS={'document_type':'invoice','issuer':'Synthetic Atlas Paper','document_number':'INV-1048','currency':'USD','total_cents':5000,'due_date':'2026-11-09','concerns':[]}
BODY={'candidates':[{'content':{'role':'model','parts':[{'text':json.dumps(FACTS)}]},'finishReason':'STOP'}],
    'modelVersion':'gemini-2.5-flash-synthetic','responseId':'response-synthetic',
    'usageMetadata':{'promptTokenCount':10,'candidatesTokenCount':4,'totalTokenCount':14},'unknown_marker':{'original':True}}


@pytest.fixture(autouse=True)
def key(monkeypatch):monkeypatch.setenv('GOOGLE_API_KEY','synthetic-not-a-provider-key')


class Script(httpx.AsyncBaseTransport):
    def __init__(self,items):self.items=iter(items);self.requests=[];self.closed=False
    async def handle_async_request(self,request):
        self.requests.append({'path':request.url.path,'body':json.loads(request.content)})
        value=next(self.items)
        if isinstance(value,BaseException):raise value
        if isinstance(value,int):return httpx.Response(value,json={'error':{'code':value,'message':'Synthetic provider unavailable'},'usageMetadata':BODY['usageMetadata']},headers={'x-request-id':'req-failure'})
        if isinstance(value,bytes):return httpx.Response(200,content=value,headers={'content-type':'application/json','x-request-id':'req-malformed'})
        return httpx.Response(200,json=value,headers={'x-request-id':'req-synthetic'})
    async def aclose(self):self.closed=True


async def extract(script,text='Fictional Atlas invoice USD 50.00'):
    extractor=ADKExtractor('gemini-2.5-flash',transport=script)
    try:return await extractor(text)
    finally:await extractor.close()


def test_normal_native_adk_records_original_body_usage_and_limits():
    script=Script([BODY]);result=asyncio.run(extract(script));observation=result.model_observation
    assert result.facts.total_cents==5000 and len(script.requests)==1 and script.closed
    config=script.requests[0]['body']['generationConfig']
    assert config['maxOutputTokens']==4096 and config['candidateCount']==1 and config['responseMimeType']=='application/json'
    assert script.requests[0]['path'].endswith('gemini-2.5-flash:generateContent')
    assert observation['response_models']==[BODY['modelVersion']] and observation['cost_usd'] is None
    raw=observation['native_observations'][0]
    assert raw['body']==BODY and raw['request_id']=='req-synthetic' and raw['response_id']=='response-synthetic'
    assert observation['reported_usage']['fields']==BODY['usageMetadata'] and observation['reported_usage']['complete'] is True
    assert len(raw['input_sha256'])==len(raw['body_sha256'])==64
    assert 'synthetic-not-a-provider-key' not in json.dumps(observation)


@pytest.mark.parametrize('finish',['MAX_TOKENS','SAFETY','OTHER',None])
def test_native_nonstop_facts_never_complete_a_job(tmp_path,finish):
    body=deepcopy(BODY);body['candidates'][0]['finishReason']=finish;script=Script([body])
    store=JobStore(tmp_path/'jobs.db');job=store.submit('native','invoice-a','Fictional invoice USD 50.00')
    async def exercise():
        extractor=ADKExtractor('gemini-2.5-flash',transport=script)
        try:return await DocumentWorker(store,extractor).run_once()
        finally:await extractor.close()
    result=asyncio.run(exercise())
    assert result['status']=='failed' and result['commit_accepted'] is True and script.closed and len(script.requests)==1
    payload=json.loads(result['result_json']);assert 'facts' not in payload and payload['job_id']==job['job_id']
    raw=payload['model_observation']['native_observations'][0]
    assert raw['finish_reason']==finish and raw['body']==body and raw['status']=='rejected'
    assert json.loads(result['attempts'][0]['observation_json'])['reported_usage']['fields']['totalTokenCount']==14


@pytest.mark.parametrize('mode',['missing','partial','zero','invalid'])
def test_native_usage_presence_remains_separate_from_defaults(mode):
    body=deepcopy(BODY)
    if mode=='missing':body.pop('usageMetadata')
    elif mode=='partial':body['usageMetadata']={'totalTokenCount':0}
    elif mode=='zero':body['usageMetadata']=dict.fromkeys(BODY['usageMetadata'],0)
    else:body['usageMetadata']={**BODY['usageMetadata'],'promptTokenCount':True}
    script=Script([body])
    try:observation=asyncio.run(extract(script)).model_observation
    except ExtractionError as error:observation=error.model_observation
    assert observation['native_observations'][0]['body']==body and script.closed
    assert observation['reported_usage']['complete'] is (mode=='zero')
    assert observation['reported_usage']['fields']['totalTokenCount']==(None if mode=='missing' else 0 if mode in {'partial','zero'} else 14)


def test_native_http_error_is_one_attempt_and_measurements_survive(tmp_path):
    script=Script([503])
    with pytest.raises(ExtractionError) as error:asyncio.run(extract(script))
    observation=error.value.model_observation;raw=observation['native_observations'][0]
    assert raw['http_status']==503 and raw['request_id']=='req-failure' and raw['body']['error']['code']==503
    assert observation['reported_usage']['fields']['totalTokenCount']==14 and len(script.requests)==1 and script.closed


@pytest.mark.parametrize('mode',['empty','multiple','invalid-json','bool-cents','function','blocked','safety-blocked','wrong-role','oversized'])
def test_native_malformed_or_unsupported_output_never_returns_facts(mode):
    body=deepcopy(BODY)
    if mode=='empty':body['candidates'][0]['content']['parts']=[{'text':'   '}]
    elif mode=='multiple':body['candidates'].append(deepcopy(body['candidates'][0]))
    elif mode=='invalid-json':body=b'{synthetic-invalid-json'
    elif mode=='bool-cents':body['candidates'][0]['content']['parts'][0]['text']=json.dumps({**FACTS,'total_cents':True})
    elif mode=='function':body['candidates'][0]['content']['parts'].append({'functionCall':{'name':'external_action','args':{}}})
    elif mode=='safety-blocked':body['candidates'][0]['safetyRatings']=[{'category':'HARM_CATEGORY_DANGEROUS_CONTENT','blocked':True}]
    elif mode=='wrong-role':body['candidates'][0]['content']['role']='user'
    elif mode=='oversized':body=(json.dumps(body)+' '*1_048_576).encode()
    else:body['promptFeedback']={'blockReason':'SAFETY'}
    script=Script([body])
    with pytest.raises(ExtractionError) as error:asyncio.run(extract(script))
    assert len(script.requests)==1 and script.closed and len(error.value.model_observation['native_observations'])==1


@pytest.mark.parametrize('mode',['cancel','deadline','close'])
def test_native_pending_request_cancels_closes_and_deletes_session(mode):
    async def exercise():
        entered=asyncio.Event();closed=[];cancelled=[]
        class Pending(httpx.AsyncBaseTransport):
            async def handle_async_request(self,request):
                entered.set()
                try:await asyncio.Event().wait()
                finally:cancelled.append(True)
            async def aclose(self):closed.append(True)
        extractor=ADKExtractor('gemini-2.5-flash',transport=Pending(),timeout_seconds=0.04 if mode=='deadline' else 120)
        task=asyncio.create_task(extractor('Fictional pending invoice'))
        await entered.wait()
        with pytest.raises(RuntimeError,match='active document'):await extractor('Other fictional invoice')
        if mode=='cancel':task.cancel()
        elif mode=='close':await extractor.close()
        with pytest.raises(ExtractionError if mode=='deadline' else asyncio.CancelledError) as error:await task
        assert closed and cancelled and error.value.model_observation['native_observations'][0]['status']=='failed'
        sessions=await extractor.runner.session_service.list_sessions(app_name='document_processing',user_id='worker')
        assert sessions.sessions==[]
        await extractor.close()
    asyncio.run(exercise())


def test_owned_native_client_closes_on_runner_startup_failure(monkeypatch):
    script=Script([])
    def broken(*_):raise RuntimeError('synthetic runner startup failure')
    monkeypatch.setattr(agent_module,'build_runner',broken)
    with pytest.raises(ExtractionError,match='startup failure'):asyncio.run(extract(script))
    assert script.closed and script.requests==[]
