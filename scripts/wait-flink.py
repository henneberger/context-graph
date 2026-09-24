#!/usr/bin/env python3
"""Wait for a running Flink job and a successful checkpoint, using explicit context."""
import argparse,json,subprocess,time
p=argparse.ArgumentParser();p.add_argument('--context',required=True);p.add_argument('--name',default='context-secure-v1');p.add_argument('--timeout',type=int,default=300);a=p.parse_args();base=['kubectl','--context',a.context,'-n','context-graph'];deadline=time.monotonic()+a.timeout
while time.monotonic()<deadline:
    try:
        status=json.loads(subprocess.check_output(base+['get','flinkdeployment',a.name,'-o','json']))['status']
        job=status.get('jobStatus',{})
        if job.get('state')=='RUNNING' and status.get('lifecycleState')=='STABLE':
            url=f'/api/v1/namespaces/context-graph/services/{a.name}-rest:8081/proxy/jobs/{job["jobId"]}/checkpoints'
            checkpoints=json.loads(subprocess.check_output(base+['get','--raw',url],stderr=subprocess.DEVNULL))
            if checkpoints['counts']['completed']>0:
                print(a.name+' RUNNING/STABLE with completed checkpoint');break
    except (KeyError,subprocess.CalledProcessError,json.JSONDecodeError):pass
    time.sleep(5)
else:raise SystemExit('Flink did not reach RUNNING/STABLE with completed checkpoint')
