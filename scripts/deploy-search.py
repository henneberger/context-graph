#!/usr/bin/env python3
"""Stage the local search release without replaying unrelated infrastructure migrations."""
import argparse,json,subprocess,sys,os
from pathlib import Path
import yaml
from render import documents
from kube_forward import service_forward
import permissions
p=argparse.ArgumentParser();p.add_argument('--context',default='docker-desktop');p.add_argument('--node',default='docker-desktop');a=p.parse_args();base=['kubectl','--context',a.context,'-n','context-graph']
policy=json.loads(subprocess.check_output(base+['get','networkpolicy','kubernetes-api-egress','-o','json']));cidrs=[x['ipBlock']['cidr'] for x in policy['spec']['egress'][0]['to']]
docs=list(documents(node=a.node,api_cidrs=cidrs));Path('.runtime/search-rendered.yaml').write_text(yaml.safe_dump_all(docs,sort_keys=False))
# Source schema and broker ACLs must exist before workers can publish.
with service_forward(a.context,'spicedb',8443) as host:
 os.environ['SPICEDB_ADMIN_ENDPOINT']='https://'+host
 permissions.api('/v1/schema/write',{'schema':Path('config/security/schema.zed').read_text()})
topic_job=yaml.safe_load(Path('deploy/k8s/secure-topics.yaml').read_text())
topic_job['metadata']['name']='search-topics-v1'
subprocess.run(base+['apply','-f','-'],input=yaml.safe_dump(topic_job),text=True,check=True)
subprocess.run(base+['wait','--for=condition=complete','job/search-topics-v1','--timeout=180s'],check=True)
names={'temporal','temporal-tls-proxy','connectors','search-api','search-ui','control-ui','ingestion','query','control','dashboard'}
selected=[d for d in docs if d['kind'] in ('ConfigMap','NetworkPolicy') or d['metadata']['name'] in names and d['kind'] not in ('Job',)]
# Boundaries are applied before new source credentials are mounted into workers.
for group in ([d for d in selected if d['kind']=='NetworkPolicy'],[d for d in selected if d['kind']!='NetworkPolicy']):
 subprocess.run(base+['apply','-f','-'],input=yaml.safe_dump_all(group),text=True,check=True)
subprocess.run(base+['rollout','restart','deployment/identity'],check=True)
for name in ('identity','ingestion','query'):
 subprocess.run(base+['rollout','status','deployment/'+name,'--timeout=180s'],check=True)
# Operator takes a savepoint when the source-topic config changes; stable UIDs preserve offsets.
flink=next(d for d in docs if d['kind']=='FlinkDeployment');subprocess.run(base+['apply','-f','-'],input=yaml.safe_dump(flink),text=True,check=True)
for name in ('control','control-ui','search-api','search-ui','connectors'):
 subprocess.run(base+['rollout','status','deployment/'+name,'--timeout=240s'],check=True)
subprocess.run([sys.executable,'scripts/wait-flink.py','--context',a.context,'--name','context-secure-v1'],check=True)
print('Search release deployed.')
