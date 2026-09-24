#!/usr/bin/env python3
"""Verify real pod isolation in a disposable namespace; exit nonzero when unenforced."""
import argparse,json,subprocess,time,uuid

def check(context):
    namespace='cg-netcheck-'+uuid.uuid4().hex[:8]
    base=['kubectl','--context',context]
    def run(*args,check=True,input=None):
        return subprocess.run(base+list(args),input=input,text=True,capture_output=True,check=check)
    def apply(value):run('apply','-f','-',input=json.dumps(value))
    apply({'apiVersion':'v1','kind':'Namespace','metadata':{'name':namespace}})
    try:
        for role,command in [('server',['sh','-ec','mkdir /www; echo ready >/www/index.html; httpd -f -p 8080 -h /www']),('client',['sleep','180'])]:
            apply({'apiVersion':'v1','kind':'Pod','metadata':{'name':role,'namespace':namespace,'labels':{'role':role}},'spec':{'automountServiceAccountToken':False,'restartPolicy':'Never','containers':[{'name':role,'image':'busybox:1.37','command':command,'resources':{'requests':{'cpu':'5m','memory':'8Mi'},'limits':{'memory':'32Mi'}}}]}})
        run('-n',namespace,'wait','--for=condition=Ready','pod/server','pod/client','--timeout=60s')
        ip=run('-n',namespace,'get','pod/server','-o','jsonpath={.status.podIP}').stdout
        def reachable():return run('-n',namespace,'exec','client','--','wget','-q','-T','2','-O','-',f'http://{ip}:8080/',check=False).returncode==0
        if not reachable():raise RuntimeError('Network probe baseline failed before deny policy')
        apply({'apiVersion':'networking.k8s.io/v1','kind':'NetworkPolicy','metadata':{'name':'deny-server','namespace':namespace},'spec':{'podSelector':{'matchLabels':{'role':'server'}},'policyTypes':['Ingress'],'ingress':[]}})
        for attempt in range(8):
            time.sleep(2)
            if not reachable():
                print('PASS NetworkPolicy actually blocks pod-to-pod traffic');return
        raise RuntimeError('NetworkPolicy is NOT enforced by this cluster CNI; install/configure a policy-capable CNI before secure deployment')
    finally:run('delete','namespace',namespace,'--wait=false',check=False)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--context',required=True);a=p.parse_args()
    try:check(a.context)
    except (RuntimeError,subprocess.CalledProcessError) as error:raise SystemExit(str(error))
