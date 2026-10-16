"""Native Gemini JSON evidence, captured before Google SDK normalization."""
from datetime import datetime, timezone
import hashlib
import json

import httpx
from google import genai
from google.genai import types

MAX_BODY=1_048_576
FIELDS=('promptTokenCount','candidatesTokenCount','totalTokenCount')


def reported_usage(body):
    raw=body.get('usageMetadata') if isinstance(body,dict) else None
    values={field:raw.get(field) if isinstance(raw,dict) and type(raw.get(field)) is int and 0<=raw[field]<=2**53-1 else None for field in FIELDS}
    return {'fields':values,'complete':all(v is not None for v in values.values()),'cost_usd':None}


def aggregate_usage(observations):
    usage=[o.get('reported_usage') for o in observations]
    fields={field:sum(u['fields'][field] for u in usage) if usage and all(u and u['fields'][field] is not None for u in usage) else None for field in FIELDS}
    fields={key:value if value is None or value<=2**53-1 else None for key,value in fields.items()}
    return {'attempts':len(observations),'measured_attempts':sum(bool(u and u['complete']) for u in usage),
        'complete':bool(usage) and all(u and u['complete'] for u in usage) and all(v is not None for v in fields.values()),'fields':fields,'cost_usd':None}


class NativeEvidence:
    def __init__(self):self.observations=[];self.callback=None

    def publish(self):
        if self.callback:self.callback()

    async def request(self,request):
        raw=await request.aread();body=json.loads(raw)
        self.observations.append({'attempt':len(self.observations)+1,'status':'started',
            'started_at':datetime.now(timezone.utc).isoformat(),'finished_at':None,
            'input_sha256':hashlib.sha256(raw).hexdigest(),'body':None,'reported_usage':None,
            'generation_config':body.get('generationConfig'),'http_status':None,'request_id':None,'response_model':None,'response_id':None,'finish_reason':None,'error':None})
        self.publish()

    async def response(self,response):
        item=self.observations[-1]
        item.update(http_status=response.status_code,request_id=response.headers.get('x-request-id') or response.headers.get('request-id'),status='received')
        try:
            # Public aread also leaves bytes available to the actual SDK. This
            # evidence cap applies after HTTP buffering, not on the network.
            raw=await response.aread();item['body_sha256']=hashlib.sha256(raw).hexdigest()
            item['truncated']=len(raw)>MAX_BODY
            text=raw[:MAX_BODY].decode('utf8',errors='replace')
            try:body=json.loads(text)
            except ValueError:body=text
            item.update(body=body,body_source='HTTP decoded response bytes before SDK validation',reported_usage=reported_usage(body))
            if isinstance(body,dict):
                candidates=body.get('candidates')
                candidate=candidates[0] if isinstance(candidates,list) and candidates and isinstance(candidates[0],dict) else {}
                item.update(response_model=body.get('modelVersion'),response_id=body.get('responseId'),finish_reason=candidate.get('finishReason'))
            self.publish()
            if item['truncated']:raise ValueError('Gemini response exceeds 1 MiB evidence limit')
            if 200<=response.status_code<300:
                validate_completion(body);item['status']='accepted'
            else:item['status']='http_error'
            self.publish()
        except BaseException as error:
            item.update(status='rejected',error={'name':type(error).__name__,'message':str(error)})
            self.publish();raise
        finally:
            item['finished_at']=datetime.now(timezone.utc).isoformat()
            await response.aclose()

    def failure(self,error):
        if self.observations and self.observations[-1]['status'] in {'started','received'}:
            self.observations[-1].update(status='failed',finished_at=datetime.now(timezone.utc).isoformat(),error={'name':type(error).__name__,'message':str(error)})


def validate_completion(body):
    if not isinstance(body,dict):raise ValueError('Gemini response is not JSON object')
    feedback=body.get('promptFeedback')
    if isinstance(feedback,dict) and feedback.get('blockReason'):raise ValueError('Gemini prompt was blocked')
    candidates=body.get('candidates')
    if not isinstance(candidates,list) or len(candidates)!=1 or not isinstance(candidates[0],dict):raise ValueError('Gemini must return exactly one candidate')
    candidate=candidates[0]
    if candidate.get('finishReason')!='STOP':raise ValueError(f"Gemini response did not finish ({candidate.get('finishReason')!r})")
    if any(isinstance(rating,dict) and rating.get('blocked') is True for rating in candidate.get('safetyRatings',[])):raise ValueError('Gemini candidate was blocked')
    content=candidate.get('content',{});parts=content.get('parts') if isinstance(content,dict) else None
    if isinstance(content,dict) and content.get('role') not in {None,'model'}:raise ValueError('Gemini content has wrong role')
    if not isinstance(parts,list) or not parts or any(not isinstance(p,dict) for p in parts):raise ValueError('Gemini response lacks content')
    if any(any(part.get(key) is not None for key in ('functionCall','executableCode','codeExecutionResult','inlineData','fileData')) for part in parts):raise ValueError('Extraction returned unsupported executable/nontext content')
    if not any(not part.get('thought') and isinstance(part.get('text'),str) and part['text'].strip() for part in parts):raise ValueError('Gemini response has no final text')


async def create_client(key,evidence,*,transport=None,base_url='https://generativelanguage.googleapis.com'):
    http=httpx.AsyncClient(transport=transport,timeout=httpx.Timeout(30,connect=5),event_hooks={'request':[evidence.request],'response':[evidence.response]})
    try:
        client=genai.Client(api_key=key,vertexai=False,http_options=types.HttpOptions(base_url=base_url,api_version='v1beta',
            timeout=30000,retry_options=types.HttpRetryOptions(attempts=1),httpx_async_client=http))
        return client,http
    except BaseException:
        await http.aclose();raise
