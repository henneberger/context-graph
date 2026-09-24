#!/usr/bin/env python3
"""Exercise platform auth, read-only behavior, live inventory and actual metric families."""
import argparse,json,sys,urllib.request,urllib.error,ssl,subprocess
from pathlib import Path
from kube_forward import service_forward
p=argparse.ArgumentParser();p.add_argument('--context',default='docker-desktop');p.add_argument('--url',default='http://localhost:18088');p.add_argument('--output',default='docs/evidence/control-plane.json');a=p.parse_args()
creds=json.loads(Path('.runtime/security/credentials.json').read_text());ca=ssl.create_default_context(cafile='.runtime/security/ca.crt')
def request(url,body=None,token=None,method=None):
 headers={'Content-Type':'application/json'}
 if token:headers['Authorization']='Bearer '+token
 req=urllib.request.Request(url,data=json.dumps(body).encode() if body is not None else None,headers=headers,method=method)
 try:r=urllib.request.urlopen(req,context=ca,timeout=60)
 except urllib.error.HTTPError as e:r=e
 with r:return r.status,r.read()
def login(user):
 status,body=request(a.url+'/auth/token',{'username':user,'password':creds[user]});assert status==200,'login failed';return json.loads(body)['access_token']
admin=login('admin');alice=login('alice');out={}
for verb,resource,expected in [('list','deployments','yes'),('get','secrets','no'),('create','pods/exec','no'),('patch','deployments','no')]:
 result=subprocess.run(['kubectl','--context',a.context,'-n','context-graph','auth','can-i',verb,resource,'--as=system:serviceaccount:context-graph:control'],capture_output=True,text=True)
 assert result.stdout.strip()==expected,(verb,resource,result.stdout)
out['liveReadOnlyRBAC']=True
with service_forward(a.context,'control',8443) as host:
 url='https://'+host+'/control/api/snapshot'
 for name,token,expected in [('anonymous',None,401),('workspaceMember',alice,403),('forged',admin[:-8]+'xxxxxxxx',401)]:
  status,_=request(url,token=token);assert status==expected,(name,status);out[name+'Denied']=True
 status,body=request(url,token=admin);assert status==200,('admin',status);snapshot=json.loads(body)
 assert not snapshot['errors'],snapshot['errors'];assert snapshot['readOnly'];assert snapshot['flink'];assert any(d['file']=='queries.yaml' for d in snapshot['definitions']);assert any(d['file']=='ingestion.json' for d in snapshot['definitions'])
 for method in ('POST','PUT','PATCH','DELETE'):
  status,_=request(url,{},admin,method);assert status==405,(method,status)
 out['readOnly']=True;out['liveInventory']=True;out['workloads']=len(snapshot['workloads']);out['configuredDefinitions']=len(snapshot['definitions'])
 # Revoke platform permission while retaining ordinary workspace membership.
 sys.path.insert(0,'scripts');import permissions,os
 with service_forward(a.context,'spicedb',8443) as spice:
  os.environ['SPICEDB_ADMIN_ENDPOINT']='https://'+spice
  grant=permissions.relationship('workspace','platform','administrator','user','admin')
  try:
   permissions.write([grant],delete=True);status,_=request(url,token=admin);assert status==403,status;out['immediatePlatformRevocation']=True
  finally:permissions.write([grant])
with service_forward(a.context,'prometheus',9090) as host:
 def prom(path):return json.load(urllib.request.urlopen('http://'+host+path,timeout=15))['data']
 targets=prom('/api/v1/targets')['activeTargets'];out['targets']=[{'job':t['labels']['job'],'health':t['health']} for t in targets]
 assert targets and all(t['health']=='up' for t in targets),[(t['labels']['job'],t['health']) for t in targets]
 families=prom('/api/v1/label/__name__/values')
 expected={'api':'context_http_requests_total','flink':'flink_','kafka':'kafka_broker_','postgres':'pg_stat_database_','polaris':'http_server_requests_','spicedb':'spicedb_','dashboard':'nginx_http_requests_total','rustfs':'rustfs_'}
 out['metricFamilies']={k:any(n.startswith(prefix) for n in families) for k,prefix in expected.items()}
 assert all(out['metricFamilies'].values()),out['metricFamilies']
 status,body=request(a.url+'/control/api/snapshot',token=admin);assert status==200;out['dashboardProxy']=True
Path(a.output).write_text(json.dumps(out,indent=2)+'\n');print('PASS control authorization, read-only inventory and service metric families.')
