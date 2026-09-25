#!/usr/bin/env python3
"""Destructively replace local application data with the schema-defined runtime; no migration."""
import argparse,json,subprocess,sys,time,uuid
from pathlib import Path
import requests,yaml
from kube_forward import service_forward
from render import documents

p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--context',required=True);p.add_argument('--node',required=True)
p.add_argument('--reset-data',action='store_true',required=True,help='Acknowledge deletion of the old application datasets and Kafka streams')
a=p.parse_args();base=['kubectl','--context',a.context,'-n','context-graph']
def k(*args,**kwargs):return subprocess.run(base+list(args),check=True,text=True,**kwargs)
# Refuse destructive reset until replacement images are present locally.
for service in ('processor','ingestion','query','dashboard'):
 subprocess.run(['docker','image','inspect',f'context-graph/{service}:schema-v1'],check=True,stdout=subprocess.DEVNULL)
workloads=['ingestion','query','connectors','context-secure-v1-jobmanager','context-secure-v1-taskmanager']
k('scale',*[f'deployment/{name}' for name in workloads],'--replicas=0')
for role in ('ingestion','query','processor'):
 pods=json.loads(k('get','pods','-l','context-graph-role='+role,'-o','json',capture_output=True).stdout)['items']
 for pod in pods:
  if pod['status']['phase'] not in ('Succeeded','Failed'):
   k('wait','--for=delete','pod/'+pod['metadata']['name'],'--timeout=120s')
# Remove catalog registrations and their data files. Identity/permission databases remain intact.
with service_forward(a.context,'polaris',8443) as address:
 session=requests.Session();session.verify='.runtime/security/ca.crt';origin='https://'+address
 secret=json.loads(Path('.runtime/security/lakehouse-credentials.json').read_text())['polaris_root_secret']
 r=session.post(origin+'/api/catalog/v1/oauth/tokens',data={'grant_type':'client_credentials','scope':'PRINCIPAL_ROLE:ALL','client_id':'root','client_secret':secret},timeout=20);r.raise_for_status()
 session.headers['Authorization']='Bearer '+r.json()['access_token']
 management=origin+'/api/management/v1/catalogs/context'
 r=session.get(management,timeout=30);r.raise_for_status();catalog=r.json();original=dict(catalog['properties'])
 r=session.put(management,json={'currentEntityVersion':catalog['entityVersion'],'properties':{**original,'polaris.config.drop-with-purge.enabled':'true'}},timeout=30);r.raise_for_status()
 try:
  path='/api/catalog/v1/context/namespaces/context_secure/tables'
  r=session.get(origin+path,timeout=30);r.raise_for_status()
  for table in r.json()['identifiers']:
   name=table['name']
   r=session.delete(origin+path+'/'+name,params={'purgeRequested':'true'},timeout=30);r.raise_for_status()
   print('Removed table:',name,flush=True)
 finally:
  r=session.get(management,timeout=30);r.raise_for_status();catalog=r.json()
  r=session.put(management,json={'currentEntityVersion':catalog['entityVersion'],'properties':original},timeout=30);r.raise_for_status()
# Use mounted administration credentials without copying or printing them.
reset='''set -eu
K=/opt/kafka/bin
for topic in events images video slack github metrics nodes edges errors rows.events rows.images rows.video rows.slack rows.github; do
 "$K/kafka-topics.sh" --bootstrap-server secure-kafka:9092 --command-config /app/secrets/kafka.properties --delete --if-exists --topic "cg.secure.$topic"
done
'''
# A one-shot Job uses the same restricted provisioner identity and mounted admin Secret.
provision=next(d for d in yaml.safe_load_all(Path('deploy/k8s/secure-topics.yaml').read_text()) if d and d['kind']=='Job')
reset_job=json.loads(json.dumps(provision));reset_job['metadata']['name']='schema-data-reset'
reset_job['spec']['template']['spec']['containers'][0]['args']=[reset]
k('delete','job/schema-data-reset','--ignore-not-found');k('apply','-f','-',input=yaml.safe_dump(reset_job))
k('wait','--for=condition=complete','job/schema-data-reset','--timeout=180s')
# Apply only rebuilt components; never retag unrelated services.
selected=[]
reset_id="context-schema-"+uuid.uuid4().hex[:12]
for d in documents(node=a.node,image_tag='schema-v1'):
 kind=d['kind'];name=d['metadata']['name']
 if kind=='ConfigMap' and name.startswith(('context-config-ingestion-','context-config-query-','context-config-processor-')):selected.append(d)
 elif kind=='Deployment' and name in ('ingestion','query','dashboard','context-secure-v1-jobmanager','context-secure-v1-taskmanager'):selected.append(d)
 elif kind=='Job' and name=='secure-topics-schema-v1':selected.append(d)
# A destructive reset must never recover offsets or a serialized prior job graph.
for d in selected:
 if d['kind']=='Deployment' and d['metadata']['name'].startswith('context-secure-v1-'):
  env=d['spec']['template']['spec']['containers'][0]['env']
  properties=next(e for e in env if e['name']=='FLINK_PROPERTIES')
  values=yaml.safe_load(properties['value'])
  values.update({'high-availability.cluster-id':reset_id,'kubernetes.cluster-id':reset_id,'high-availability.storageDir':'s3://context-recovery/ha/'+reset_id})
  properties['value']=yaml.safe_dump(values)
k('delete','job/secure-topics-schema-v1','--ignore-not-found')
k('apply','-f','-',input=yaml.safe_dump_all(selected))
k('wait','--for=condition=complete','job/secure-topics-schema-v1','--timeout=180s')
for name in ('ingestion','query','dashboard','context-secure-v1-jobmanager','context-secure-v1-taskmanager'):
 k('rollout','status','deployment/'+name,'--timeout=240s')
# Clear only connector ingestion acknowledgments so example adapters perform a fresh scrape.
# The connector remains stopped until this succeeds; no previous document content is copied.
pod=json.loads(subprocess.check_output(base+['get','pods','-l','app=spicedb-postgres','-o','json']))['items'][0]['metadata']['name']
k('exec',pod,'--','sh','-c','psql -U "$POSTGRES_USER" -d connectors -c "TRUNCATE documents;"')
k('scale','deployment/connectors','--replicas=2')
print('Fresh schema runtime deployed. Run authenticated acceptance checks before treating this as validated.')
