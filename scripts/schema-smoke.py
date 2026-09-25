#!/usr/bin/env python3
import sys,json,time,uuid,datetime,requests
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parent))
from kube_forward import service_forward
import argparse
p=argparse.ArgumentParser(description='Validate native JSON Schema / VARIANT storage and access control against a running platform.')
p.add_argument('--url',default='http://localhost:18088')
p.add_argument('--context',default='docker-desktop')
p.add_argument('--credentials',default='.runtime/security/credentials.json')
a=p.parse_args()
base=a.url.rstrip('/')
creds=json.loads(Path(a.credentials).read_text())
def headers(user):
 r=requests.post(base+'/auth/token',json={'username':user,'password':creds[user]},timeout=20);r.raise_for_status()
 return {'Authorization':'Bearer '+r.json()['access_token'],'X-Workspace-Id':'demo','X-Resource-Key':'alpha'}
h={u:headers(u) for u in ['producer','alice','bob']}
marker='variant-'+uuid.uuid4().hex
body={'eventTime':datetime.datetime.now(datetime.timezone.utc).isoformat(),'metric':marker,'value':7.25,'arbitrary':{'nested':[1,True,None,{'hello':'world'}]}}
r=requests.post(base+'/ingest/events',json=body,headers=h['producer'],timeout=30);assert r.status_code==202,(r.status_code,r.text)
r=requests.post(base+'/ingest/events',json={'value':'invalid'},headers=h['producer'],timeout=30);assert r.status_code==400,r.status_code
q='{records(entityId:"alpha",limit:100){metric value _json_remainder}}'
for i in range(36):
 r=requests.post(base+'/graphql',json={'query':q},headers=h['alice'],timeout=40);data=r.json()
 rows=(data.get('data') or {}).get('records') or []
 found=[v for v in rows if v['metric']==marker]
 if found:
  assert found[0]['_json_remainder']=={'arbitrary':body['arbitrary']},found
  assert found[0]['value']==7.25,found
  print('PASS JSON Schema validation and authenticated Kafka -> Flink -> Iceberg VARIANT -> DuckDB -> GraphQL round trip',flush=True);break
 if i==35:raise AssertionError(data)
 time.sleep(5)
r=requests.post(base+'/graphql',json={'query':q},headers=h['bob'],timeout=40)
assert r.status_code==200 and not r.json().get('errors'),r.json()
assert r.json()['data']['records']==[],r.json()
r=requests.post(base+'/ingest/events',json=body,headers=h['bob'],timeout=30);assert r.status_code==403
r=requests.post(base+'/graphql',json={'query':q},timeout=30);assert r.status_code==401
print('PASS disjoint resource reads/writes and anonymous reads denied',flush=True)
# Advance this input's event-time watermark beyond the reference SQL window.
future=(datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(seconds=30)).isoformat()
r=requests.post(base+'/ingest/events',json={'eventTime':future,'metric':'watermark-'+marker,'value':0},headers=h['producer'],timeout=30);assert r.status_code==202
for i in range(36):
 r=requests.post(base+'/graphql',json={'query':'query($m:String!){metricTotals(metric:$m){sum_value sample_count}}','variables':{'m':marker}},headers=h['alice'],timeout=40)
 data=r.json();rows=(data.get('data') or {}).get('metricTotals') or []
 if rows:
  assert rows[0]['sum_value']==7.25 and rows[0]['sample_count']==1,rows
  print('PASS schema-defined temporal SQL aggregation',flush=True);break
 if i==35:raise AssertionError(data)
 time.sleep(5)
with service_forward(a.context,'context-secure-v1',8081) as host:
 j=requests.get('http://'+host+'/jobs/overview').json()['jobs'];assert j and all(x['state']=='RUNNING' for x in j),j
 for job in j:
  c=requests.get('http://'+host+'/jobs/'+job['jid']+'/checkpoints').json();assert c['counts']['completed']>0,c['counts'];print('PASS Flink running with completed checkpoints:',c['counts']['completed'])
