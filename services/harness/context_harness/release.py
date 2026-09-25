"""Explicit operator-side releases. Builder artifacts never contain deployment credentials."""
import copy,json,subprocess,tempfile,time,sys
from pathlib import Path
import yaml,requests
from .compiler import ROOT,Invalid,compile_bundle,identifier
NS='context-graph'
IMAGES={'ingestion':'context-graph/ingestion:harness-v1','query':'context-graph/query:harness-v3','processor':'context-graph/processor:harness-v2'}
def kubectl(context,*args,**kwargs):return subprocess.run(['kubectl','--context',context,'-n',NS,*args],check=True,text=True,**kwargs)
def load(path):return json.loads((Path(path)/'bundle.json').read_text())
def meta(name,labels=None):return {'name':name,'namespace':NS,'labels':labels or {}}
def svc(name,selector,ports):return {'apiVersion':'v1','kind':'Service','metadata':meta(name),'spec':{'selector':selector,'ports':[{'name':n,'port':p,'targetPort':p} for n,p in ports]}}
def render(path,node):
    b=load(path);release=b['release'];identifier(release,r'cg-[a-z0-9-]{1,57}');identifier(node,r'[a-z0-9][a-z0-9.-]{0,252}')
    data={};items=[]
    for p in sorted(path.rglob('*')):
        if p.is_file() and (p.name in ('ingestion.json','queries.yaml','query.yaml') or p.parent.name=='schemas' or p.name in [j['name'].removeprefix(release+'-')+'.json' for j in b['jobs']]):
            rel=p.relative_to(path).as_posix();key=rel.replace('/','__');data[key]=p.read_text();items.append({'key':key,'path':rel})
    cm='context-config-'+release
    docs=[{'apiVersion':'v1','kind':'ConfigMap','metadata':meta(cm,{'context-graph-bundle':release}),'immutable':True,'data':data}]
    inventory={k:b[k] for k in ('release','name','workspace','namespace','digest','inputs','jobs')}
    docs.append({'apiVersion':'v1','kind':'ConfigMap','metadata':meta(release+'-inventory',{'context-graph-bundle':release,'context-graph-inventory':'true'}),'immutable':True,'data':{'bundle.json':json.dumps(inventory)}})
    bases=[d for d in yaml.safe_load_all((ROOT/'deploy/k8s/apps.yaml').read_text()) if d and d['kind']=='Deployment']
    for role in ('ingestion','query'):
        d=copy.deepcopy(next(d for d in bases if d['metadata']['name']==role));name=release+'-'+role;d['metadata']=meta(name,{'context-graph-bundle':release});d['spec']['replicas']=1
        d['spec']['selector']['matchLabels']={'app':name};pod=d['spec']['template'];pod['metadata']['labels']={'app':name,'context-graph-role':role,'context-graph-bundle':release}
        ps=pod['spec'];ps['automountServiceAccountToken']=False;ps['nodeSelector']={'kubernetes.io/hostname':node}
        c=ps['containers'][0];c['image']=IMAGES[role];c.pop('lifecycle',None)
        c['env']=[v for v in c['env'] if not v['name'].startswith('MEDIA_S3_')]
        c['env'].append({'name':'REQUIRED_WORKSPACE','value':b['workspace']})
        for e in c['env']:
            if e['name']=='POLARIS_NAMESPACE':e['value']=b['namespace']
            ref=e.get('valueFrom',{}).get('secretKeyRef',{})
            if ref.get('name')=='security-'+role:ref['name']=name
            if ref.get('name')=='polaris-query':ref['name']=release+'-catalog-query'
        if role=='query':c['env'].append({'name':'BUNDLE_INGESTION_URL','value':'https://'+release+'-ingestion:8080'})
        for volume in ps['volumes']:
            if volume.get('configMap',{}).get('name')=='context-config':volume['configMap']={'name':cm,'items':items}
            if volume.get('secret',{}).get('secretName')=='security-'+role:volume['secret']['secretName']=name
            if 'persistentVolumeClaim' in volume:volume.pop('persistentVolumeClaim');volume['emptyDir']={}
        docs.extend([d,svc(name,{'app':name},[('https',8080 if role=='ingestion' else 8081)])])
    flink=next(d for d in yaml.safe_load_all((ROOT/'deploy/legacy/flink-operator.yaml').read_text()) if d and d['kind']=='FlinkDeployment')['spec']
    for job in b['jobs']:
        cluster=job['name'];alias=cluster.removeprefix(release+'-');settings=copy.deepcopy(flink['flinkConfiguration'])
        settings={k:v for k,v in settings.items() if not k.startswith('kubernetes.operator.') and not k.startswith('kubernetes.jobmanager.') and not k.startswith('kubernetes.taskmanager.')}
        settings.update({'jobmanager.rpc.address':cluster+'-jm','rest.address':cluster+'-jm','rest.bind-address':'0.0.0.0','rest.port':'8081','jobmanager.rpc.port':'6123',
          'blob.server.port':'6124','taskmanager.rpc.port':'6122','taskmanager.data.port':'6121','taskmanager.numberOfTaskSlots':str(job['parallelism']),
          'jobmanager.memory.process.size':'768m','taskmanager.memory.process.size':'1024m','high-availability.cluster-id':cluster,
          'high-availability.storageDir':'s3://context-recovery/ha/'+cluster,'execution.checkpointing.dir':'s3://context-recovery/checkpoints/'+cluster,
          'execution.checkpointing.savepoint-dir':'s3://context-recovery/savepoints/'+cluster,'kubernetes.namespace':NS,'kubernetes.cluster-id':cluster})
        for component in ('jm','tm'):
            pod=copy.deepcopy(flink['podTemplate']);ps=pod['spec'];name=cluster+'-'+component
            pod['metadata']['labels']={'app':cluster,'component':component,'context-graph-role':'processor','context-graph-bundle':release,'context-graph-control-plane':'true'}
            ps['nodeSelector']={'kubernetes.io/hostname':node};ps['serviceAccountName']='flink-runtime';c=ps['containers'][0];c['name']='flink';c['image']=IMAGES['processor']
            c['args']=['standalone-job','--job-classname','io.contextgraph.processor.SqlBundleJob','/app/config/'+alias+'.json'] if component=='jm' else ['taskmanager']
            c['env'].append({'name':'JAVA_TOOL_OPTIONS','value':settings['env.java.opts.all']})
            c['env'].append({'name':'FLINK_PROPERTIES','value':'\n'.join(k+': '+str(v) for k,v in settings.items() if k!='env.java.opts.all')})
            c['resources']={'requests':{'cpu':'100m','memory':'512Mi' if component=='jm' else '768Mi'},'limits':{'memory':'1Gi' if component=='jm' else '1280Mi'}}
            for e in c['env']:
                if e['name']=='JOB_ID':e['value']=cluster
                if e['name']=='CHECKPOINT_URI':e['value']='s3://context-recovery/checkpoints/'+cluster
                if e['name']=='SAVEPOINT_URI':e['value']='s3://context-recovery/savepoints/'+cluster
                ref=e.get('valueFrom',{}).get('secretKeyRef',{})
                if ref.get('name')=='polaris-processor':ref['name']=release+'-catalog-processor'
            if component=='jm':c['readinessProbe']={'httpGet':{'path':'/overview','port':8081},'periodSeconds':5,'timeoutSeconds':3}
            for v in ps['volumes']:
                if 'configMap' in v:v['configMap']={'name':cm,'items':items}
                if 'secret' in v:v['secret']['secretName']=release+'-processor'
            docs.append({'apiVersion':'apps/v1','kind':'Deployment','metadata':meta(name,{'context-graph-bundle':release}),
              'spec':{'replicas':1,'strategy':{'type':'Recreate'},'selector':{'matchLabels':{'app':cluster,'component':component}},'template':pod}})
        docs.append(svc(cluster+'-jm',{'app':cluster,'component':'jm'},[('rest',8081),('rpc',6123),('blob',6124)]))
    # Existing platform policies cover Kafka, identity, SpiceDB, catalog, storage, DNS and metrics.
    # These rules cover private bundle routes and Flink standalone internal communication.
    selectors=[{'podSelector':{'matchLabels':{'context-graph-bundle':release}}}]
    docs.append({'apiVersion':'networking.k8s.io/v1','kind':'NetworkPolicy','metadata':meta(release+'-internal'),
      'spec':{'podSelector':{'matchLabels':{'context-graph-bundle':release}},'policyTypes':['Ingress','Egress'],
      'ingress':[{'from':selectors+[{'podSelector':{'matchLabels':{'app':'control'}}}], 'ports':[{'protocol':'TCP','port':p} for p in (8080,8081,6121,6122,6123,6124)]}],
      'egress':[{'to':selectors,'ports':[{'protocol':'TCP','port':p} for p in (8080,8081,6121,6122,6123,6124)]}]}})
    return docs

def fresh_release(path):
    """Recompile authored sources; never deploy a user-edited generated manifest."""
    b=load(path);tmp=tempfile.TemporaryDirectory(prefix='context-release-');root=Path(tmp.name)/'source';root.mkdir()
    for name,text in b['sourceFiles'].items():
        target=(root/name).resolve()
        if not target.is_relative_to(root.resolve()):raise Invalid('Invalid source path')
        target.parent.mkdir(parents=True,exist_ok=True);target.write_text(text)
    target=Path(tmp.name)/'compiled';compile_bundle(root/b.get('entrypoint','bundle.yaml'),target)
    if load(target)['digest']!=b['digest']:raise Invalid('Source digest mismatch')
    return tmp,target

def deploy(args):
    tmp,path=fresh_release(args.release)
    try:
        from .provision import provision
        b=load(path);provision(b,args.context)
        docs=render(path,args.node)
        for subset in ([d for d in docs if d['kind']=='NetworkPolicy'],[d for d in docs if d['kind']!='NetworkPolicy']):
            kubectl(args.context,'apply','-f','-',input=yaml.safe_dump_all(subset),stdout=subprocess.DEVNULL)
        # Record reproducible rendered metadata locally, without any Secret values.
        (Path(args.release)/'rendered.yaml').write_text(yaml.safe_dump_all(docs,sort_keys=False))
        print('Deployed candidate '+b['release']+'; use cg status before promotion.')
    finally:tmp.cleanup()

def status(args):
    b=load(args.release);kubectl(args.context,'get','deployments,pods','-l','context-graph-bundle='+b['release'])
    sys.path.insert(0,str(ROOT/'scripts'));from kube_forward import service_forward
    for job in b['jobs']:
        with service_forward(args.context,job['name']+'-jm',8081) as endpoint:
            r=requests.get('http://'+endpoint+'/jobs/overview',timeout=10);r.raise_for_status();print(json.dumps({'job':job['name'],'runtime':r.json()}))

def promote(args):
    b=load(args.release)
    # Refuse routing to incomplete workloads; Flink completion/checkpoint is also required.
    sys.path.insert(0,str(ROOT/'scripts'));from kube_forward import service_forward
    for job in b['jobs']:
        with service_forward(args.context,job['name']+'-jm',8081) as endpoint:
            jobs=requests.get('http://'+endpoint+'/jobs/overview',timeout=10).json()['jobs']
            if len(jobs)!=1 or jobs[0]['state']!='RUNNING':raise Invalid('Candidate Flink job is not RUNNING')
            cp=requests.get('http://'+endpoint+'/jobs/'+jobs[0]['jid']+'/checkpoints',timeout=10).json()
            if cp.get('counts',{}).get('completed',0)<1:raise Invalid('Candidate requires a completed checkpoint')
    for role in ('ingestion','query'):
        kubectl(args.context,'rollout','status','deployment/'+b['release']+'-'+role,'--timeout=120s',stdout=subprocess.DEVNULL)
    # Each endpoint cutover is explicit; Kubernetes provides no atomic multi-Service switch.
    docs=[svc('cg-'+b['name']+'-'+r,{'app':b['release']+'-'+r},[('https',8080 if r=='ingestion' else 8081)]) for r in ('ingestion','query')]
    kubectl(args.context,'apply','-f','-',input=yaml.safe_dump_all(docs),stdout=subprocess.DEVNULL)
    print('Promoted '+b['release']+'. Prior version retained for explicit rollback.')
