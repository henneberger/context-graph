#!/usr/bin/env python3
"""Live imported-source isolation and server-side lease expiry, with disposable relationships."""
import datetime as dt,json,os,time,uuid
from pathlib import Path
from kube_forward import service_forward
import permissions as p
identifier='source-smoke-'+uuid.uuid4().hex
with service_forward('docker-desktop','spicedb',8443) as host:
 os.environ['SPICEDB_ADMIN_ENDPOINT']='https://'+host
 rid=p.resource_id(identifier,'document')
 tuples=[p.relationship('workspace',identifier,'administrator','user','admin'),p.relationship('entity',rid,'workspace','workspace',identifier),p.relationship('entity',rid,'imported','user','*'),p.relationship('entity',rid,'source','source',identifier),p.relationship('source',identifier,'reader','user','*')]
 tuples[-1]['optionalExpiresAt']=(dt.datetime.now(dt.timezone.utc)+dt.timedelta(seconds=8)).isoformat()
 def write(items,operation):return p.api('/v1/relationships/write',{'updates':[{'operation':operation,'relationship':r} for r in items]})
 def allowed(user):return p.api('/v1/permissions/check',{'consistency':{'fullyConsistent':True},'resource':{'objectType':'entity','objectId':rid},'permission':'view','subject':{'object':{'objectType':'user','objectId':user}}})['permissionship']=='PERMISSIONSHIP_HAS_PERMISSION'
 try:
  write(tuples[:-1],'OPERATION_TOUCH');assert not allowed('admin'),'Workspace administrator bypassed imported-source ACL'
  write(tuples[-1:],'OPERATION_TOUCH');assert allowed('admin');assert not allowed('outsider'),'Public source bypassed workspace membership'
  time.sleep(9);assert not allowed('admin'),'Expired lease still grants access'
  Path('docs/evidence/source-permissions.json').write_text(json.dumps({'importedAdminBypassDenied':True,'workspaceIsolation':True,'validLeaseGranted':True,'serverSideExpiryDenied':True},indent=2)+'\n')
  print('PASS imported-source isolation, workspace boundary and server-side lease expiry.')
 finally:write(tuples,'OPERATION_DELETE')
