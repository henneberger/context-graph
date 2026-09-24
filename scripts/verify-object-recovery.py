#!/usr/bin/env python3
"""Savepoint/restart Flink without legacy data mounts and prove S3 restore + new checkpoint.
Ingestion remains live; Kafka buffers during the job pause. Retained PVC data is never deleted.
"""
import argparse,json,socket,subprocess,time,urllib.request
from pathlib import Path
from contextlib import ExitStack
from kube_forward import service_forward
p=argparse.ArgumentParser(description=__doc__);p.add_argument('--context',default='docker-desktop');p.add_argument('--name',default='context-secure-v1');p.add_argument('--output',default='docs/evidence/object-recovery.json');a=p.parse_args()
base=['kubectl','--context',a.context,'-n','context-graph']
def read():return json.loads(subprocess.check_output(base+['get','flinkdeployment',a.name,'-o','json']))
def patch(value):subprocess.run(base+['patch','flinkdeployment',a.name,'--type=merge','-p',json.dumps({'spec':value})],check=True,stdout=subprocess.DEVNULL)
def wait(check,seconds=240):
 deadline=time.monotonic()+seconds
 while time.monotonic()<deadline:
  result=check()
  if result:return result
  time.sleep(2)
 raise TimeoutError('Flink recovery verification deadline exceeded')
original=read();assert original['status']['jobStatus']['state']=='RUNNING','A running job is required'
backup=Path('.runtime/object-recovery-original-spec.json');backup.write_text(json.dumps(original['spec']));backup.chmod(0o600)
started=time.monotonic();forwards=ExitStack()
try:
 patch({'job':{'state':'suspended','upgradeMode':'savepoint'}})
 def suspended():
  d=read();j=d['status']['jobStatus'];path=j.get('upgradeSavepointPath','')
  return path if d['status'].get('observedGeneration')==d['metadata']['generation'] and j['state'] in ('FINISHED','SUSPENDED') and path.startswith('s3://context-recovery/') else None
 savepoint=wait(suspended)
 print('Completed S3 savepoint; resuming without legacy PVC mounts.',flush=True)
 template=original['spec']['podTemplate'];spec=template['spec']
 removed={v['name'] for v in spec.get('volumes',[]) if v.get('persistentVolumeClaim',{}).get('claimName')=='context-data'}
 spec['volumes']=[v for v in spec.get('volumes',[]) if v['name'] not in removed]
 for c in spec.get('containers',[]):c['volumeMounts']=[m for m in c.get('volumeMounts',[]) if m['name'] not in removed]
 patch({'podTemplate':template,'job':{'state':'running','upgradeMode':'savepoint'}})
 def running():
  d=read();return d if d['status'].get('observedGeneration')==d['metadata']['generation'] and d['status']['jobStatus']['jobId']!=original['status']['jobStatus']['jobId'] and d['status']['jobStatus']['state']=='RUNNING' and d['status'].get('reconciliationStatus',{}).get('state')=='DEPLOYED' else None
 current=wait(running);job=current['status']['jobStatus']['jobId']
 host=forwards.enter_context(service_forward(a.context,a.name+'-rest',8081))
 def checkpoint():
  try:
   d=json.load(urllib.request.urlopen(f'http://{host}/jobs/{job}/checkpoints',timeout=5));latest=d.get('latest',{});restored=latest.get('restored') or {};completed=latest.get('completed') or {}
   if restored.get('external_path')==savepoint and completed.get('external_path','').startswith('s3://context-recovery/'):return {'restored':restored,'completed':completed}
  except (OSError,ValueError):pass
  return None
 proof=wait(checkpoint,120)
 pods=json.loads(subprocess.check_output(base+['get','pods','-l','app='+a.name,'-o','json']))
 assert pods['items'] and not any(v.get('persistentVolumeClaim',{}).get('claimName')=='context-data' for pod in pods['items'] for v in pod['spec'].get('volumes',[])),'Legacy volume still mounted'
 result={'status':'passed','savepoint':savepoint,'jobId':job,'legacyDataVolumeMounted':False,'elapsedSeconds':round(time.monotonic()-started,2),**proof}
 Path(a.output).write_text(json.dumps(result,indent=2));print('PASS restored from S3 and completed a new S3 checkpoint with no legacy data volume.')
except Exception:
 # Restore the retained, known-compatible mounts on failure; never abandon a suspended writer.
 original=json.loads(backup.read_text());original['job']['state']='running';patch(original)
 raise
finally:
 forwards.close()
