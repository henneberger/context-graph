#!/usr/bin/env python3
"""Prove API network access is limited to Kafka, identity, and permission checks."""
import argparse,json,subprocess,uuid
p=argparse.ArgumentParser();p.add_argument('--context',required=True);a=p.parse_args()
base=['kubectl','--context',a.context,'-n','context-graph'];name='policy-proof-'+uuid.uuid4().hex[:8]
pod={'apiVersion':'v1','kind':'Pod','metadata':{'name':name,'namespace':'context-graph','labels':{'context-graph-role':'query'}},'spec':{'automountServiceAccountToken':False,'restartPolicy':'Never','containers':[{'name':'probe','image':'busybox:1.37','command':['sleep','120'],'resources':{'requests':{'cpu':'5m','memory':'8Mi'},'limits':{'memory':'32Mi'}}}]}}
subprocess.run(base+['apply','-f','-'],input=json.dumps(pod),text=True,check=True)
try:
 subprocess.run(base+['wait','--for=condition=Ready','pod/'+name,'--timeout=60s'],check=True)
 for host,port,expected in [('spicedb-checks',8443,True),('identity',8443,True),('secure-kafka',9092,True),('spicedb',8443,False),('spicedb-postgres',5432,False)]:
  result=subprocess.run(base+['exec',name,'--','nc','-z','-w','2',host,str(port)],capture_output=True)
  reachable=result.returncode==0
  if reachable != expected:raise SystemExit(f'FAIL {host}: reachable={reachable}, expected={expected}')
  print(f'PASS {host}: '+('allowed' if reachable else 'blocked'))
finally:subprocess.run(base+['delete','pod',name,'--wait=false'],check=True)
