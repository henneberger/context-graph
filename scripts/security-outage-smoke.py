#!/usr/bin/env python3
"""Temporarily break the check proxy's backend DNS, assert fail-closed APIs, restore.
Run only against the disposable local development namespace with operator authorization.
"""
import argparse
import importlib.util
import json
from pathlib import Path
import subprocess
import time

spec=importlib.util.spec_from_file_location('security_smoke',Path(__file__).with_name('security-smoke.py'))
smoke=importlib.util.module_from_spec(spec);spec.loader.exec_module(smoke)
parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--context',required=True)
parser.add_argument('--url',default='http://localhost:18088')
parser.add_argument('--output',default='.runtime/security-outage-smoke.json')
args=parser.parse_args()
config=argparse.Namespace(url=args.url,credentials='.runtime/security/credentials.json',spice_token='.runtime/security/spicedb-token',ca='.runtime/security/ca.crt',token_path='/auth/token',spicedb_url='https://localhost:18443',identity_url='https://localhost:18444')
client=smoke.Smoke(config)
command=['kubectl','--context',args.context,'-n','context-graph']

def rollout(endpoint):
    previous={p['metadata']['name'] for p in json.loads(subprocess.check_output(command+['get','pods','-l','app=spicedb-checks','-o','json']))['items']}
    subprocess.run(command+['set','env','deployment/spicedb-checks','SPICEDB_ENDPOINT='+endpoint],check=True,stdout=subprocess.DEVNULL)
    subprocess.run(command+['rollout','status','deployment/spicedb-checks','--timeout=120s'],check=True,stdout=subprocess.DEVNULL)
    deadline=time.monotonic()+90
    while time.monotonic()<deadline:
        current={p['metadata']['name'] for p in json.loads(subprocess.check_output(command+['get','pods','-l','app=spicedb-checks','-o','json']))['items']}
        if not previous & current: return
        time.sleep(1)
    raise TimeoutError('Prior proxy pods did not fully terminate; outage is not isolated')

manifest=json.loads(subprocess.check_output(command+['get','deployment','spicedb-checks','-o','json']))
original=next(e['value'] for e in manifest['spec']['template']['spec']['containers'][0]['env'] if e['name']=='SPICEDB_ENDPOINT')
if original!='https://spicedb:8443':
    raise SystemExit('Expected healthy local development SpiceDB backend before test')
client.query('alice','{nodes(limit:10){node_id}}')
media=client.query('alice','{media(entityId:"alpha",limit:100){kind payload}}')['media']
image=next(row for row in media if row['kind']=='image')
payload=json.loads(image['payload']) if isinstance(image['payload'],str) else image['payload']
uri=payload['uri']
client.token('producer')
try:
    rollout('https://unavailable-authorization.invalid:8443')
    client.expect('queryFailsClosedOnAuthorizationOutage',client.request('/graphql',{'query':'{nodes(limit:10){node_id}}'},user='alice'),{503})
    client.expect('ingestionFailsClosedOnAuthorizationOutage',client.request('/ingest/events',{'entityId':'alpha','value':1},user='producer'),{503})
    client.expect('mediaFailsClosedOnAuthorizationOutage',client.request(uri,user='alice'),{503})
finally:
    rollout(original)
client.query('alice','{nodes(limit:10){node_id}}')
client.expect('mediaRecoveredAfterAuthorizationRestore',client.request(uri,user='alice'),{200})
client.evidence['status']='passed'
Path(args.output).write_text(json.dumps(client.evidence,indent=2))
print('PASS real authorization dependency outage denied query, ingestion and media; backend restored.')
