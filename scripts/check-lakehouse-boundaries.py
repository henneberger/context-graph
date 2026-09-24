#!/usr/bin/env python3
"""Exercise enforced network boundaries from three service roles; no credentials needed."""
import argparse,json,subprocess,uuid
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--context',default='docker-desktop');p.add_argument('--output',default='docs/evidence/lakehouse-network.json');a=p.parse_args();base=['kubectl','--context',a.context,'-n','context-graph'];evidence={}
for role,checks in {'query':[('polaris',8443,True),('object-storage',9000,True),('spicedb-postgres',5432,False)],'ingestion':[('object-storage',9000,True),('polaris',8443,False)],'dashboard':[('object-storage',9000,False),('polaris',8443,False)]}.items():
 name='lakehouse-proof-'+uuid.uuid4().hex[:8]
 pod={'apiVersion':'v1','kind':'Pod','metadata':{'name':name,'namespace':'context-graph','labels':{'context-graph-role':role}},'spec':{'automountServiceAccountToken':False,'restartPolicy':'Never','containers':[{'name':'probe','image':'busybox:1.37','command':['sleep','120'],'resources':{'requests':{'cpu':'5m','memory':'8Mi'},'limits':{'memory':'32Mi'}}}]}}
 subprocess.run(base+['apply','-f','-'],input=json.dumps(pod),text=True,check=True,stdout=subprocess.DEVNULL)
 try:
  subprocess.run(base+['wait','--for=condition=Ready','pod/'+name,'--timeout=60s'],check=True,stdout=subprocess.DEVNULL)
  for host,port,allowed in checks:
   result=subprocess.run(base+['exec',name,'--','nc','-z','-w','2',host,str(port)],capture_output=True);reachable=result.returncode==0
   assert reachable==allowed,f'{role} -> {host}: expected {allowed}, got {reachable}'
   evidence[role+'->'+host]='allowed' if reachable else 'blocked'
 finally:subprocess.run(base+['delete','pod',name,'--wait=false'],check=True,stdout=subprocess.DEVNULL)
Path(a.output).write_text(json.dumps(evidence,indent=2));print(json.dumps(evidence))
