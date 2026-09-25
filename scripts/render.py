#!/usr/bin/env python3
"""Render Kubernetes resources with content-addressed configuration and pinned local node."""
import argparse, copy, hashlib, json
from pathlib import Path
import yaml
from configmap import render

def documents(config_root=Path('config'), node=None, image_tag='secure-v6', api_cidrs=()):
    complete,all_items=render(config_root,'context-config')
    configurations={}
    for role,filenames in {
        'ingestion': {'ingestion.json'},
        'processor': {'jobs.yaml'},
        'query': {'query.yaml','queries.yaml'},
        'connectors': {'connectors/sources.yaml'}
    }.items():
        selected=[item for item in all_items if item['path'] in filenames or (role in ('ingestion','processor') and item['path'].startswith('schemas/'))]
        config=copy.deepcopy(complete)
        config['data']={item['key']:complete['data'][item['key']] for item in selected}
        digest=hashlib.sha256(json.dumps(config['data'],sort_keys=True).encode()).hexdigest()[:12]
        config['metadata']['name']='context-config-'+role+'-'+digest
        configurations[role]=(config,selected)
    yield yaml.safe_load(Path('deploy/k8s/namespace.yaml').read_text())
    for config,_ in configurations.values(): yield config
    for filename in ['storage.yaml','security-services.yaml','security-check-proxy.yaml','lakehouse.yaml','secure-kafka.yaml','secure-topics.yaml','apps.yaml','flink.yaml','control.yaml','observability.yaml','network-policies.yaml','monitoring-policies.yaml','search.yaml','search-policies.yaml']:
      for document in yaml.safe_load_all(Path('deploy/k8s',filename).read_text()):
        if not document: continue
        role='processor' if document['kind']=='FlinkDeployment' or document.get('spec',{}).get('template',{}).get('metadata',{}).get('labels',{}).get('context-graph-role')=='processor' else document.get('metadata',{}).get('name','')
        config,items=configurations.get(role,configurations['ingestion'])
        def visit(value):
          if isinstance(value,dict):
            if 'configMap' in value and value['configMap'].get('name')=='context-config':
              value['configMap'].update(name=config['metadata']['name'],items=copy.deepcopy(items))
            if isinstance(value.get('image'),str) and value['image'].startswith('context-graph/') and (image_tag!='secure-v6' or not any(pin in value['image'] for pin in (':lakehouse-',':observability-',':search-',':harness-',':ax-',':schema-'))):
              value['image']=value['image'].rsplit(':',1)[0]+':'+image_tag
            for child in value.values():visit(child)
          elif isinstance(value,list):
            for child in value:visit(child)
        visit(document)
        if document['kind']=='FlinkDeployment':
            document['spec']['podTemplate'].setdefault('metadata',{}).setdefault('labels',{})['context-graph-role']='processor'
            document['spec']['podTemplate']['metadata']['labels']['context-graph-control-plane']='true'
        elif document['kind']=='Deployment' and role in ('ingestion','query','dashboard','identity'):
            document['spec']['template']['metadata'].setdefault('labels',{})['context-graph-role']=role
        if document['kind']=='NetworkPolicy' and document['metadata']['name']=='kubernetes-api-egress':
            document['spec']['egress'][0]['to']=[{'ipBlock':{'cidr':cidr}} for cidr in api_cidrs] or [{'ipBlock':{'cidr':'127.0.0.1/32'}}]
        if node:
          if document['kind'] in ('Deployment','StatefulSet','Job'):document['spec']['template']['spec']['nodeSelector']={'kubernetes.io/hostname':node}
          elif document['kind']=='FlinkDeployment':document['spec']['podTemplate']['spec']['nodeSelector']={'kubernetes.io/hostname':node}
          elif document['kind']=='PersistentVolume':document['spec']['nodeAffinity']={'required':{'nodeSelectorTerms':[{'matchExpressions':[{'key':'kubernetes.io/hostname','operator':'In','values':[node]}]}]}}
        yield document

if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--node',required=True,help='Single storage node hostname; every workload is pinned here');p.add_argument('--config',type=Path,default=Path('config'));p.add_argument('--image-tag',default='secure-v6');p.add_argument('--api-server-cidr',action='append',default=[]);a=p.parse_args();print(yaml.safe_dump_all(documents(a.config,a.node,a.image_tag,a.api_server_cidr),sort_keys=False))
