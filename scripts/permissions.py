#!/usr/bin/env python3
"""Trusted operator provisioning. Do not expose this API or its SpiceDB credential to users."""
import argparse,hashlib,json,os,sys,re,unicodedata
from pathlib import Path
import requests
ROOT=Path('.runtime/security')
def resource_id(workspace,entity):
    if not re.fullmatch(r'[A-Za-z0-9_-]{1,64}',workspace):raise ValueError('Invalid workspace identifier')
    if not entity or not entity.strip() or len(entity.encode('utf-16-le'))//2>256 or any(unicodedata.category(c)=='Cc' for c in entity):raise ValueError('Invalid entity identifier')
    return hashlib.sha256((workspace+'\0'+entity).encode()).hexdigest()
def relationship(kind,id,relation,subject_kind,subject_id,subject_relation=None):
    subject={'object':{'objectType':subject_kind,'objectId':subject_id}}
    if subject_relation:subject['optionalRelation']=subject_relation
    return {'resource':{'objectType':kind,'objectId':id},'relation':relation,'subject':subject}
def api(path,body,endpoint=None):
    response=requests.post((endpoint or os.environ.get('SPICEDB_ADMIN_ENDPOINT','https://localhost:18443'))+path,headers={'Authorization':'Bearer '+(ROOT/'spicedb-token').read_text().strip()},json=body,verify=str(ROOT/'ca.crt'),timeout=15)
    if not response.ok:raise RuntimeError(f'SpiceDB returned {response.status_code}: {response.text[:400]}')
    return response.json()
def workspace_bindings(resource):
    body={'consistency':{'fullyConsistent':True},'relationshipFilter':{'resourceType':'entity','optionalResourceId':resource,'optionalRelation':'workspace'}}
    response=requests.post(os.environ.get('SPICEDB_ADMIN_ENDPOINT','https://localhost:18443')+'/v1/relationships/read',headers={'Authorization':'Bearer '+(ROOT/'spicedb-token').read_text().strip()},json=body,verify=str(ROOT/'ca.crt'),timeout=15,stream=True)
    response.raise_for_status()
    bindings=[]
    try:
        for line in response.iter_lines():
            if not line:continue
            result=json.loads(line)
            if 'error' in result:raise RuntimeError('Workspace binding lookup failed')
            result=result.get('result',result)
            if 'relationship' in result:bindings.append(result['relationship'])
            if len(bindings)>1:raise RuntimeError('Entity has multiple workspace relationships; operator repair required')
    finally:response.close()
    return bindings

def ensure_entity_workspace(workspace,entity,create=False):
    resource=resource_id(workspace,entity);bindings=workspace_bindings(resource)
    expected=relationship('entity',resource,'workspace','workspace',workspace)
    if bindings:
        subject=bindings[0].get('subject',{}).get('object',{})
        if subject.get('objectType')!='workspace' or subject.get('objectId')!=workspace:raise RuntimeError('Entity belongs to a different workspace; reassociation denied')
        return resource
    if not create:raise RuntimeError('Entity is not provisioned; use the entity command first')
    # The absence precondition and creation are one SpiceDB transaction. Two racing
    # creators cannot add two workspace relationships through this tool.
    api('/v1/relationships/write',{'optionalPreconditions':[{'operation':'OPERATION_MUST_NOT_MATCH','filter':{'resourceType':'entity','optionalResourceId':resource,'optionalRelation':'workspace'}}],
        'updates':[{'operation':'OPERATION_CREATE','relationship':expected}]})
    return resource

def write(items,delete=False):
    if any(item.get('resource',{}).get('objectType')=='entity' and item.get('relation')=='workspace' for item in items):raise ValueError('Workspace relationships require guarded entity provisioning')
    return api('/v1/relationships/write',{'updates':[{'operation':'OPERATION_DELETE' if delete else 'OPERATION_TOUCH','relationship':item} for item in items]})
def bootstrap():
    api('/v1/schema/write',{'schema':Path('config/security/schema.zed').read_text()})
    tuples=[relationship('workspace','demo','administrator','user','admin'),relationship('workspace','platform','administrator','user','admin')]
    for name in ('alice','bob','producer'):tuples.append(relationship('workspace','demo','member','user',name))
    tuples.append(relationship('workspace','other','member','user','outsider'))
    for entity in ('alpha','beta','shared'):
        rid=ensure_entity_workspace('demo',entity,create=True);tuples.append(relationship('entity',rid,'writer','user','producer'))
        if entity!='shared':tuples.append(relationship('entity',rid,'restricted','user','*'))
    tuples.extend([relationship('entity',resource_id('demo','alpha'),'reader','user','alice'),relationship('entity',resource_id('demo','beta'),'reader','user','bob')])
    rid=ensure_entity_workspace('other','alpha',create=True);tuples.append(relationship('entity',rid,'reader','user','outsider'))
    write(tuples)
    print('Provisioned demo workspace, shared entity, restricted alpha/beta, and isolated other workspace.')
if __name__=='__main__':
    p=argparse.ArgumentParser();sub=p.add_subparsers(dest='command',required=True);sub.add_parser('bootstrap')
    for action in ('entity','grant','revoke','restrict','unrestrict'):
        q=sub.add_parser(action);q.add_argument('--workspace',required=True);q.add_argument('--entity',required=True)
        if action in ('grant','revoke'):q.add_argument('--user',required=True);q.add_argument('--relation',choices=['reader','writer'],default='reader')
    a=p.parse_args()
    if a.command=='bootstrap':bootstrap()
    elif a.command=='entity':
        ensure_entity_workspace(a.workspace,a.entity,create=True);print(json.dumps({'provisioned':True}))
    else:
        ensure_entity_workspace(a.workspace,a.entity)
        if a.command in ('restrict','unrestrict'):item=relationship('entity',resource_id(a.workspace,a.entity),'restricted','user','*')
        else:item=relationship('entity',resource_id(a.workspace,a.entity),a.relation,'user',a.user)
        result=write([item],a.command in ('revoke','unrestrict'));print(json.dumps({'updated':True,'writtenAt':result.get('writtenAt')}))
