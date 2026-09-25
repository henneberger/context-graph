#!/usr/bin/env python3
"""Wait for an operator-free Flink application and its first completed checkpoint."""
import argparse,time,requests
from kube_forward import service_forward
p=argparse.ArgumentParser();p.add_argument('--context',required=True);p.add_argument('--name',default='context-secure-v1');p.add_argument('--service');p.add_argument('--timeout',type=int,default=300);a=p.parse_args()
with service_forward(a.context,a.service or a.name+'-rest',8081) as endpoint:
 deadline=time.monotonic()+a.timeout
 while time.monotonic()<deadline:
  try:
   jobs=requests.get('http://'+endpoint+'/jobs/overview',timeout=5).json()['jobs']
   if len(jobs)==1 and jobs[0]['state']=='RUNNING':
    checkpoints=requests.get('http://'+endpoint+'/jobs/'+jobs[0]['jid']+'/checkpoints',timeout=5).json()
    if checkpoints.get('counts',{}).get('completed',0)>0:print(a.name+' RUNNING with completed checkpoint');break
  except (requests.RequestException,KeyError,ValueError):pass
  time.sleep(3)
 else:raise SystemExit('Job did not reach RUNNING with a completed checkpoint')
