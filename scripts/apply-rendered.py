#!/usr/bin/env python3
"""Apply a release; safely recreate its two idempotent provisioning Jobs on reapply."""
import argparse,json,subprocess,yaml
p=argparse.ArgumentParser();p.add_argument('--context',required=True);p.add_argument('--manifest',required=True);a=p.parse_args();base=['kubectl','--context',a.context]
docs=[d for d in yaml.safe_load_all(open(a.manifest)) if d]
jobs=[d for d in docs if d['kind']=='Job']
for job in jobs:
 if job['metadata']['name'] not in ('secure-topics-v1','spicedb-migrate-v1562','polaris-bootstrap-v170'):raise SystemExit('Refusing automatic recreation of unrecognized Job')
# Secrets and policies have already been prepared by deploy.sh.
subprocess.run(base+['apply','-f','-'],input=yaml.safe_dump_all([d for d in docs if d['kind']!='Job']),text=True,check=True)
for job in jobs:
 namespace=job['metadata']['namespace'];name=job['metadata']['name'];cmd=base+['-n',namespace]
 result=subprocess.run(cmd+['get','job',name,'--ignore-not-found','-o','json'],capture_output=True,text=True,check=True)
 if result.stdout.strip():
  old=json.loads(result.stdout);conditions=old.get('status',{}).get('conditions',[])
  if name=='polaris-bootstrap-v170':
   if not any(c['type']=='Complete' and c['status']=='True' for c in conditions):subprocess.run(cmd+['wait','--for=condition=complete','job/'+name,'--timeout=300s'],check=True)
   continue
  terminal=any(c['type'] in ('Complete','Failed') and c['status']=='True' for c in conditions)
  if not terminal:subprocess.run(cmd+['wait','--for=condition=complete','job/'+name,'--timeout=300s'],check=True)
  subprocess.run(cmd+['delete','job',name,'--wait=true','--timeout=60s'],check=True)
 subprocess.run(base+['apply','-f','-'],input=yaml.safe_dump(job),text=True,check=True)
