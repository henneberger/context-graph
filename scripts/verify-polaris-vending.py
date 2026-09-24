#!/usr/bin/env python3
"""Repeatable positive/negative checks for real Polaris-vended RustFS credentials.
Uses private local service credentials. Never prints tokens or storage credentials.
"""
import argparse,json,socket,subprocess,time,uuid
from pathlib import Path
import boto3,requests
from botocore.config import Config
p=argparse.ArgumentParser(description=__doc__);p.add_argument('--context',default='docker-desktop');p.add_argument('--output',default='docs/evidence/polaris-vending.json');a=p.parse_args()
root=Path('.runtime/security');base=['kubectl','--context',a.context,'-n','context-graph'];processes=[];logs=[]
from contextlib import ExitStack
from kube_forward import service_forward
stack=ExitStack()
def forward(service,remote):return 'https://'+stack.enter_context(service_forward(a.context,service,remote))
try:
 catalog=forward('polaris',8443)+'/api/catalog/v1';storage=forward('object-storage',9000)
 session=requests.Session();session.verify=str(root/'ca.crt');creds=json.loads((root/'polaris-query.json').read_text())['credentials']
 r=session.post(catalog+'/oauth/tokens',data={'grant_type':'client_credentials','scope':'PRINCIPAL_ROLE:ALL','client_id':creds['clientId'],'client_secret':creds['clientSecret']},timeout=15);r.raise_for_status();session.headers['Authorization']='Bearer '+r.json()['access_token']
 table=catalog+'/context/namespaces/context_secure/tables/events'
 r=session.get(table,headers={'X-Iceberg-Access-Delegation':'vended-credentials'},timeout=20);r.raise_for_status();data=r.json();v=data['config']
 s3=boto3.client('s3',endpoint_url=storage,aws_access_key_id=v['s3.access-key-id'],aws_secret_access_key=v['s3.secret-access-key'],aws_session_token=v['s3.session-token'],verify=str(root/'ca.crt'),region_name='us-east-1',config=Config(s3={'addressing_style':'path'},connect_timeout=5,read_timeout=10,retries={'max_attempts':1}))
 key=data['metadata-location'].split('/',3)[3]
 evidence={'vendedSessionToken':bool(v['s3.session-token']),'unexpired':int(v['s3.session-token-expires-at-ms'])>int(time.time()*1000),'metadataRead':s3.head_object(Bucket='context-warehouse',Key=key)['ResponseMetadata']['HTTPStatusCode']}
 for label,call in [('otherBucket',lambda:s3.head_object(Bucket='context-media',Key='forbidden')),('recoveryBucket',lambda:s3.head_object(Bucket='context-recovery',Key='forbidden')),('otherPrefix',lambda:s3.head_object(Bucket='context-warehouse',Key='outside/forbidden')),('write',lambda:s3.put_object(Bucket='context-warehouse',Key=key.rsplit('/',1)[0]+'/permission-probe-'+uuid.uuid4().hex,Body=b'permission probe'))]:
  try:call();evidence[label]='UNEXPECTED_ALLOW'
  except s3.exceptions.ClientError as e:evidence[label]=e.response['ResponseMetadata']['HTTPStatusCode']
 r=session.get(table+'/credentials',timeout=20);evidence['credentialRefresh']=r.status_code
 r=session.post(catalog+'/context/namespaces/context_secure/tables',json={'name':'permission_probe_'+uuid.uuid4().hex,'schema':{'type':'struct','schema-id':0,'fields':[{'id':1,'name':'id','type':'long','required':True}]}},timeout=20);evidence['readerCreateTable']=r.status_code
 evidence['anonymousCatalog']=requests.get(table,verify=str(root/'ca.crt'),timeout=15).status_code
 Path(a.output).write_text(json.dumps(evidence,indent=2))
 assert evidence['vendedSessionToken'] and evidence['unexpired'] and evidence['metadataRead']==200
 assert all(evidence[k]==403 for k in ['otherBucket','recoveryBucket','otherPrefix','write','readerCreateTable']),evidence
 assert evidence['credentialRefresh']==200 and evidence['anonymousCatalog'] in (401,403),evidence
 print('PASS scoped temporary credentials, credential refresh, write/prefix/bucket denial and private catalog authentication.')
finally:
 stack.close()
