"""Small client for builders and workers; uses delegated credentials, never platform admin keys."""
import json
from pathlib import Path
import requests
class ContextClient:
    def __init__(self,query_url,ingestion_url,workspace,token_file,ca_file):
        if not all(u.startswith('https://') for u in (query_url,ingestion_url) if u):raise ValueError('HTTPS required')
        self.query_url=query_url;self.ingestion_url=ingestion_url;self.workspace=workspace;self.token_file=Path(token_file)
        self.ca_file=str(ca_file)
    def headers(self):
        raw=json.loads(self.token_file.read_text());token=raw.get('access_token',raw.get('token'))
        if not token:raise ValueError('Delegated token missing')
        return {'Authorization':'Bearer '+token.removeprefix('Bearer '),'X-Workspace-Id':self.workspace}
    def query(self,document,variables=None):
        r=requests.post(self.query_url,json={'query':document,'variables':variables or {}},headers=self.headers(),verify=self.ca_file,timeout=(5,45),allow_redirects=False)
        r.raise_for_status();result=r.json()
        if result.get('errors'):raise RuntimeError('Query denied or unavailable')
        return result['data']
    def ingest(self,endpoint,event):
        import re
        if not re.fullmatch(r'[a-z][a-z0-9_]*',endpoint):raise ValueError('Invalid endpoint')
        r=requests.post(self.ingestion_url+'/ingest/'+endpoint,json=event,headers=self.headers(),verify=self.ca_file,timeout=(5,45),allow_redirects=False)
        r.raise_for_status();return r.json()
    def trajectory(self,entity_id,run_id,event_type,details=None):
        return self.ingest('trajectories',{'entityId':entity_id,'runId':run_id,'eventType':event_type,'details':details or {}})
