#!/usr/bin/env python3
"""Savepoint the legacy operator job and replace it with ordinary Kubernetes resources."""
import argparse,json,subprocess,time
from pathlib import Path
import yaml
from render import documents
p=argparse.ArgumentParser();p.add_argument('--context',required=True);p.add_argument('--node',required=True);a=p.parse_args();base=['kubectl','--context',a.context,'-n','context-graph']
root=Path('.runtime/flink-migration');root.mkdir(parents=True,exist_ok=True)
def get():return json.loads(subprocess.check_output(base+['get','flinkdeployment','context-secure-v1','-o','json']))
old=get();(root/'operator.json').write_text(json.dumps(old))
subprocess.run(base+['patch','flinkdeployment','context-secure-v1','--type=merge','-p',json.dumps({'spec':{'job':{'state':'suspended'}}})],check=True)
deadline=time.monotonic()+300
while time.monotonic()<deadline:
 current=get();status=current.get('status',{});job=status.get('jobStatus',{})
 if job.get('state') in ('FINISHED','CANCELED') and status.get('reconciliationStatus',{}).get('state')=='DEPLOYED':
  savepoint=job.get('upgradeSavepointPath') or job.get('savepointInfo',{}).get('lastSavepoint',{}).get('location')
  if not savepoint:raise SystemExit('No completed savepoint; refusing removal')
  break
 time.sleep(3)
else:raise SystemExit('Savepoint suspension did not complete; legacy job retained')
(root/'restore.json').write_text(json.dumps({'savepoint':savepoint}))
docs=[]
for d in documents(node=a.node):
 if d['metadata']['name'].startswith('context-config-processor-') or d['metadata']['name'] in ('flink-runtime','context-secure-v1-jobmanager','context-secure-v1-taskmanager','context-secure-v1','context-secure-v1-rest','core-flink-internal'):
  if d['kind']=='Deployment' and d['metadata']['name'].endswith('jobmanager'):
   for e in d['spec']['template']['spec']['containers'][0]['env']:
    if e['name']=='FLINK_PROPERTIES':e['value']+='\nexecution.savepoint.path: '+savepoint+'\nexecution.savepoint.ignore-unclaimed-state: false'
  docs.append(d)
(root/'standalone.yaml').write_text(yaml.safe_dump_all(docs))
subprocess.run(base+['delete','flinkdeployment','context-secure-v1','--wait=true','--timeout=120s'],check=True)
# Wait for owned pods/services to disappear before reusing the stable service names.
subprocess.run(base+['wait','--for=delete','pod','-l','app=context-secure-v1','--timeout=120s'],check=True)
subprocess.run(base+['scale','deployment/flink-kubernetes-operator','--replicas=0'],check=True)
subprocess.run(base+['apply','-f',str(root/'standalone.yaml')],check=True)
print('Standalone restore submitted; saved recovery artifacts in .runtime/flink-migration. Verify with wait-flink.py.')
