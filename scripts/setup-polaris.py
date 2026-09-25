#!/usr/bin/env python3
"""Configure private Polaris roles; uses localhost TLS port-forward 18446, never prints credentials."""
import base64,json,subprocess,os
from pathlib import Path
import requests
root=Path('.runtime/security');session=requests.Session();session.verify=str(root/'ca.crt')
base=os.environ.get('POLARIS_SETUP_URL','https://localhost:18446');creds=json.loads((root/'lakehouse-credentials.json').read_text());catalog='context'
r=session.post(base+'/api/catalog/v1/oauth/tokens',data={'grant_type':'client_credentials','scope':'PRINCIPAL_ROLE:ALL','client_id':'root','client_secret':creds['polaris_root_secret']},timeout=20);r.raise_for_status();session.headers['Authorization']='Bearer '+r.json()['access_token']
def req(method,path,data=None,conflict=False):
 r=session.request(method,base+path,json=data,timeout=30)
 if conflict and r.status_code==409:return None
 if r.status_code>=400:raise RuntimeError(f'{method} {path}: {r.status_code} '+r.text[:500])
 return r.json() if r.content else None
m='/api/management/v1';c=m+'/catalogs/'+catalog
req('POST',m+'/catalogs',{'catalog':{'name':catalog,'type':'INTERNAL','readOnly':False,'properties':{'default-base-location':'s3://context-warehouse/'},'storageConfigInfo':{'storageType':'S3','allowedLocations':['s3://context-warehouse/'],'endpoint':'https://object-storage:9000','pathStyleAccess':True,'region':'us-east-1','stsUnavailable':False,'kmsUnavailable':True}}},True)
req('PUT',c+'/catalog-roles/catalog_admin/grants',{'type':'catalog','privilege':'CATALOG_MANAGE_CONTENT'})
req('POST','/api/catalog/v1/context/namespaces',{'namespace':['context_secure']},True)
for role,privileges in {'query':['NAMESPACE_LIST','TABLE_LIST','TABLE_READ_PROPERTIES','TABLE_READ_DATA'],'processor':['NAMESPACE_LIST','TABLE_LIST','TABLE_READ_PROPERTIES','TABLE_READ_DATA','TABLE_CREATE','TABLE_WRITE_DATA']}.items():
 name='context-'+role;f=root/('polaris-'+role+'.json')
 if not f.exists():
  result=req('POST',m+'/principals',{'principal':{'name':name},'credentialRotationRequired':False})
  f.write_text(json.dumps(result));f.chmod(0o600)
 req('POST',m+'/principal-roles',{'principalRole':{'name':name}},True)
 req('POST',c+'/catalog-roles',{'catalogRole':{'name':name}},True)
 for privilege in privileges:req('PUT',c+'/catalog-roles/'+name+'/grants',{'type':'catalog','privilege':privilege})
 req('PUT',m+'/principal-roles/'+name+'/catalog-roles/'+catalog,{'catalogRole':{'name':name}})
 req('PUT',m+'/principals/'+name+'/principal-roles',{'principalRole':{'name':name}})
 result=json.loads(f.read_text());data=result['credentials']
 secret={'apiVersion':'v1','kind':'Secret','metadata':{'name':'polaris-'+role,'namespace':'context-graph'},'stringData':{'client-id':data['clientId'],'client-secret':data['clientSecret'],'credential':data['clientId']+':'+data['clientSecret']}}
 subprocess.run(['kubectl','--context',os.environ.get('KUBE_CONTEXT','docker-desktop'),'apply','-f','-'],input=json.dumps(secret),text=True,check=True,stdout=subprocess.DEVNULL)
print('Polaris catalog and distinct read and write roles configured.')
