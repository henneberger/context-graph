#!/usr/bin/env python3
"""Prepare private local RustFS/Polaris before deploying their clients; no secret output."""
import argparse,hashlib,json,os,platform,socket,subprocess,sys,tarfile,time,urllib.request
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--context',default='docker-desktop');p.add_argument('--node',default=os.environ.get('STORAGE_NODE'));a=p.parse_args();env={**os.environ,'KUBE_CONTEXT':a.context};base=['kubectl','--context',a.context,'-n','context-graph']
def run(args,**kwargs):return subprocess.run(args,check=True,env=env,**kwargs)
if not a.node:
 nodes=json.loads(subprocess.check_output(['kubectl','--context',a.context,'get','nodes','-o','json']))['items']
 if len(nodes)!=1:p.error('--node is required for a cluster with more than one node')
 a.node=nodes[0]['metadata']['name']

# Install the native RustFS administration client, validating the release checksum.
client=Path('.runtime/bin/rc');version='v0.1.36'
if not client.exists():
 system={'Darwin':'macos','Linux':'linux'}[platform.system()];arch={'arm64':'arm64','aarch64':'arm64','x86_64':'amd64'}[platform.machine()]
 name=f'rustfs-cli-{system}-{arch}-{version}.tar.gz';url=f'https://github.com/rustfs/cli/releases/download/{version}/{name}';archive=Path('.runtime')/name
 with urllib.request.urlopen(url) as response:archive.write_bytes(response.read())
 expected=urllib.request.urlopen(url+'.sha256').read().decode().split()[0]
 if hashlib.sha256(archive.read_bytes()).hexdigest()!=expected:raise RuntimeError('RustFS client checksum mismatch')
 client.parent.mkdir(parents=True,exist_ok=True)
 with tarfile.open(archive) as tar:
  member=next(m for m in tar.getmembers() if Path(m.name).name=='rc' and m.isfile());client.write_bytes(tar.extractfile(member).read());client.chmod(0o755)
run([sys.executable,'scripts/provision-lakehouse.py'])
# A successfully bootstrapped realm must not be bootstrapped again.
import yaml
docs=list(yaml.safe_load_all(Path('deploy/k8s/lakehouse.yaml').read_text()))
# Match the main renderer, avoiding repeated pod restarts caused by removing/readding nodeSelector.
for document in docs:
 if document['kind'] in ('Deployment','StatefulSet','Job'):
  document['spec']['template']['spec']['nodeSelector']={'kubernetes.io/hostname':a.node}
result=subprocess.run(base+['get','job','polaris-bootstrap-v170','--ignore-not-found','-o','json'],capture_output=True,text=True,check=True)
if result.stdout.strip():docs=[d for d in docs if d['kind']!='Job']
run(base+['apply','-f','-'],input=yaml.safe_dump_all(docs),text=True)
run(base+['wait','--for=condition=complete','job/polaris-bootstrap-v170','--timeout=180s'])
for target in ['statefulset/object-storage','deployment/polaris']:run(base+['rollout','status',target,'--timeout=180s'])
from contextlib import ExitStack
from kube_forward import service_forward
with ExitStack() as stack:
 for service,remote,key in [('object-storage',9000,'OBJECT_STORAGE_SETUP_URL'),('polaris',8443,'POLARIS_SETUP_URL')]:
  env[key]='https://'+stack.enter_context(service_forward(a.context,service,remote))
 run([sys.executable,'scripts/setup-object-storage.py'])
 run([sys.executable,'scripts/setup-polaris.py'])
print('Private lakehouse ready. Serving clients use only their own catalog/storage identities.')
