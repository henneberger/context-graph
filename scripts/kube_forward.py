"""Temporary, loopback-only Kubernetes forwarding to a ready, non-terminating service pod."""
from contextlib import contextmanager
import json,socket,subprocess,tempfile,time

@contextmanager
def service_forward(context,service,remote,namespace='context-graph'):
 base=['kubectl','--context',context,'--request-timeout=15s','-n',namespace]
 spec=json.loads(subprocess.check_output(base+['get','service',service,'-o','json']))['spec']
 selector=','.join(k+'='+v for k,v in sorted(spec['selector'].items()))
 deadline=time.monotonic()+45;pod=None
 while time.monotonic()<deadline:
  items=json.loads(subprocess.check_output(base+['get','pods','-l',selector,'-o','json']))['items']
  ready=[p for p in items if not p['metadata'].get('deletionTimestamp') and p['status'].get('phase')=='Running' and any(c['type']=='Ready' and c['status']=='True' for c in p['status'].get('conditions',[]))]
  if ready:pod=sorted(ready,key=lambda p:p['metadata']['creationTimestamp'],reverse=True)[0]['metadata']['name'];break
  time.sleep(.5)
 if not pod:raise TimeoutError('No ready pod for '+service)
 with socket.socket() as s:s.bind(('127.0.0.1',0));port=s.getsockname()[1]
 with tempfile.TemporaryFile(mode='w+') as log:
  command=['kubectl','--context',context,'-n',namespace,'port-forward','pod/'+pod,f'{port}:{remote}','--address','127.0.0.1']
  process=subprocess.Popen(command,stdout=log,stderr=log)
  try:
   deadline=time.monotonic()+30
   while True:
    if process.poll() is not None:raise RuntimeError('Port-forward exited for '+service)
    try:
     with socket.create_connection(('127.0.0.1',port),timeout=1):break
    except OSError:
     if time.monotonic()>deadline:raise TimeoutError('Port-forward startup timed out')
     time.sleep(.2)
   yield f'localhost:{port}'
  finally:
   process.terminate()
   try:process.wait(timeout=10)
   except subprocess.TimeoutExpired:process.kill();process.wait(timeout=5)
