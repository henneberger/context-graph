#!/usr/bin/env python3
"""Render pinned upstream AX control plane for the isolated local trial."""
from pathlib import Path
import yaml
root=Path(__file__).resolve().parents[2];up=root/'.runtime/upstream-ax'
docs=[]
for file in ('ax-controller.yaml','ax-server.yaml','redis.yaml'):
 for d in yaml.safe_load_all((up/'deploy'/file).read_text()):
  if not d:continue
  if d['kind'] in ('ClusterRole','ClusterRoleBinding'):continue
  if d['kind']=='Deployment':
   ps=d['spec']['template']['spec'];ps['automountServiceAccountToken']=False
   for c in ps['containers']:
    if 'ko://' in c['image']:
     role='controller' if 'controller' in c['image'] else 'server';c['image']='localhost:5001/context-ax-'+role+(':ax-v2' if role=='controller' else ':ax-v1')
    if c['name']=='controller':
     for e in c.get('env',[]):
      if e['name']=='AX_SNAPSHOTS_BUCKET':e['value']='s3://ate-snapshots/context-search/'
  docs.append(d)
# No provider secrets are given to AX in this retrieval-only trial.
# Restrict its Kubernetes secret reader to its own namespace.
meta={'name':'ax-controller','namespace':'ax-system'}
docs.extend([{'apiVersion':'rbac.authorization.k8s.io/v1','kind':'Role','metadata':meta,'rules':[{'apiGroups':[''],'resources':['secrets'],'verbs':['get','list','watch']}]},
 {'apiVersion':'rbac.authorization.k8s.io/v1','kind':'RoleBinding','metadata':meta,'subjects':[{'kind':'ServiceAccount','name':'ax-controller','namespace':'ax-system'}],'roleRef':{'apiGroup':'rbac.authorization.k8s.io','kind':'Role','name':'ax-controller'}}])
print(yaml.safe_dump_all(docs,sort_keys=False))
