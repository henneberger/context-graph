"""Read-only namespace inventory. Never forwards arbitrary URLs, PromQL, or Kubernetes objects."""
import concurrent.futures, json, os, re, ssl, threading, time, urllib.request, urllib.error
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import jwt, yaml
from prometheus_client import Counter, Histogram, start_http_server

ROOT=Path(os.environ.get('CONTROL_SECRETS','/run/control'))
SA=Path(os.environ.get('SERVICE_ACCOUNT','/var/run/secrets/kubernetes.io/serviceaccount'))
NS=os.environ.get('NAMESPACE','context-graph')
ISSUER=os.environ.get('OIDC_ISSUER','https://identity:8443')
AUDIENCE=os.environ.get('OIDC_AUDIENCE','context-graph')
JWKS_URL=os.environ.get('OIDC_JWKS_URL',ISSUER+'/.well-known/jwks.json')
SPICE=os.environ.get('SPICEDB_ENDPOINT','https://spicedb-checks:8443')
PLATFORM=os.environ.get('PLATFORM_WORKSPACE','platform')
REQUESTS=Counter('context_control_requests_total','Control requests',['status'])
DURATION=Histogram('context_control_request_duration_seconds','Control request duration')
SLOTS=threading.BoundedSemaphore(16)

class Denied(Exception):
    def __init__(self,status): self.status=status

def fetch(url,body=None,headers=None,tls=None):
    req=urllib.request.Request(url,data=json.dumps(body).encode() if body is not None else None,headers=headers or {})
    # No redirects to alternative security endpoints or arbitrary hosts.
    opener=urllib.request.build_opener(urllib.request.HTTPSHandler(context=tls),NoRedirect())
    with opener.open(req,timeout=5) as r:
        data=r.read(8*1024*1024+1)
        if len(data)>8*1024*1024: raise ValueError('response too large')
        return json.loads(data)
class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,*args,**kwargs):return None

class Authorization:
    def __init__(self):
        self.tls=ssl.create_default_context(cafile=ROOT/'ca.crt')
        if not all(x.startswith('https://') for x in (ISSUER,JWKS_URL,SPICE)):raise ValueError('TLS required')
        self.keys={};self.fetched=0;self.lock=threading.Lock()
    def authenticate(self,authorization):
        if not authorization or not authorization.startswith('Bearer ') or len(authorization)>16384:raise Denied(401)
        token=authorization[7:]
        try:
            header=jwt.get_unverified_header(token)
            if header.get('alg')!='RS256' or header.get('crit') or not header.get('kid'):raise Denied(401)
            with self.lock:
                if time.monotonic()-self.fetched>60:
                    try:
                        document=fetch(JWKS_URL,tls=self.tls)
                        self.keys={k['kid']:jwt.PyJWK(k).key for k in document['keys'] if k.get('kty')=='RSA' and k.get('use','sig')=='sig' and k.get('alg','RS256')=='RS256' and 'd' not in k}
                        self.fetched=time.monotonic()
                    except Exception:raise Denied(503)
            key=self.keys.get(header['kid'])
            if key is None or key.key_size<2048:raise Denied(401)
            claims=jwt.decode(token,key,algorithms=['RS256'],audience=AUDIENCE,issuer=ISSUER,options={'require':['exp','sub','iss','aud']})
            sub=claims['sub']
            if not isinstance(sub,str) or not sub or sub=='*' or len(sub)>256 or any(ord(c)<32 for c in sub):raise Denied(401)
            return claims
        except Denied:raise
        except Exception:raise Denied(401)
    def check(self,claims):
        if claims['exp']<=time.time():raise Denied(401)
        body={'consistency':{'fully_consistent':True},'resource':{'object_type':'workspace','object_id':PLATFORM},'permission':'manage','subject':{'object':{'object_type':'user','object_id':claims['sub']}}}
        try:
            result=fetch(SPICE+'/v1/permissions/check',body,{'Authorization':'Bearer '+os.environ['SPICEDB_TOKEN'],'Content-Type':'application/json'},self.tls)
        except Exception:raise Denied(503)
        if result.get('permissionship')!='PERMISSIONSHIP_HAS_PERMISSION':raise Denied(403)
        if claims['exp']<=time.time():raise Denied(401)

class Inventory:
    def __init__(self):
        self.tls=ssl.create_default_context(cafile=SA/'ca.crt')
        self.base='https://kubernetes.default.svc'
    def kube(self,path):
        return fetch(self.base+path,headers={'Authorization':'Bearer '+(SA/'token').read_text().strip()},tls=self.tls)
    def listing(self,path):
        items=[]
        while True:
            page=self.kube(path+('?limit=200' if not items else '?limit=200&continue='+urllib.parse.quote(cursor,safe='')))
            items.extend(page.get('items',[]));cursor=page.get('metadata',{}).get('continue')
            if not cursor:return items
            if len(items)>5000:raise ValueError('inventory bound exceeded')
    def snapshot(self):
        paths={'deployments':f'/apis/apps/v1/namespaces/{NS}/deployments','statefulsets':f'/apis/apps/v1/namespaces/{NS}/statefulsets','pods':f'/api/v1/namespaces/{NS}/pods','services':f'/api/v1/namespaces/{NS}/services','flink':f'/apis/flink.apache.org/v1beta1/namespaces/{NS}/flinkdeployments','configs':f'/api/v1/namespaces/{NS}/configmaps'}
        data={};errors=[]
        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
            tasks={key:pool.submit(self.listing,path) for key,path in paths.items()}
            for key,task in tasks.items():
                try:data[key]=task.result()
                except Exception:data[key]=[];errors.append(key+' unavailable')
        workloads=[]
        for kind in ('deployments','statefulsets'):
            for obj in data[kind]:
                workloads.append({'name':obj['metadata']['name'],'kind':'Deployment' if kind=='deployments' else 'StatefulSet','desired':obj['spec'].get('replicas',1),'ready':obj.get('status',{}).get('readyReplicas',0),'available':obj.get('status',{}).get('availableReplicas',0),'generation':obj['metadata']['generation'],'observedGeneration':obj.get('status',{}).get('observedGeneration',0),'images':[c['image'] for c in obj['spec']['template']['spec']['containers']]})
        pods=[]
        for p in data['pods']:
            status=p.get('status',{});cs=status.get('containerStatuses',[])
            pods.append({'name':p['metadata']['name'],'app':p['metadata'].get('labels',{}).get('app',''),'phase':status.get('phase','Unknown'),'ready':sum(bool(c['ready']) for c in cs),'containers':len(p['spec']['containers']),'restarts':sum(c.get('restartCount',0) for c in cs),'reasons':[s.get('reason') for c in cs for s in c.get('state',{}).values() if s.get('reason')],'created':p['metadata']['creationTimestamp']})
        jobs=[]
        for f in data['flink']:
            s=f.get('status',{});j=s.get('jobStatus',{});spec=f['spec'];fc=spec.get('flinkConfiguration',{})
            jobs.append({'name':f['metadata']['name'],'state':j.get('state','UNKNOWN'),'jobId':j.get('jobId'),'reconciliation':s.get('reconciliationStatus',{}).get('state','UNKNOWN'),'parallelism':spec.get('job',{}).get('parallelism'),'version':spec.get('flinkVersion'),'image':spec.get('image'),'checkpointInterval':fc.get('execution.checkpointing.interval'),'incremental':fc.get('execution.checkpointing.incremental'),'savepointTimestamp':j.get('savepointInfo',{}).get('lastSavepoint',{}).get('timeStamp'),'restartFailed':fc.get('kubernetes.operator.job.restart.failed')})
        # Discover native Flink applications from labeled Kubernetes Services.
        # Never accept a caller-provided REST target.
        def native_job(service):
            name=service['metadata']['name']
            selector=service['spec'].get('selector',{})
            if not re.fullmatch(r'[a-z0-9-]+',name):return None
            if selector.get('component') not in ('jm','jobmanager'):return None
            if not any(p.get('port')==8081 for p in service['spec'].get('ports',[])):return None
            entry={'name':selector.get('app',name),'state':'UNAVAILABLE','reconciliation':'KUBERNETES','image':None}
            try:
                overview=fetch('http://'+name+':8081/jobs/overview')['jobs']
                if len(overview)==1:
                    job=overview[0];entry.update(state=job['state'],jobId=job['jid'])
                    checkpoints=fetch('http://'+name+':8081/jobs/'+job['jid']+'/checkpoints')
                    entry['completedCheckpoints']=checkpoints.get('counts',{}).get('completed',0)
                    entry['savepointTimestamp']=checkpoints.get('latest',{}).get('savepoint',{}).get('trigger_timestamp')
            except Exception:pass
            return entry
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            jobs.extend(j for j in pool.map(native_job,data['services']) if j)
        # Only deployed references and an explicit allowlist of configuration fields are returned.
        configs={c['metadata']['name']:c for c in data['configs']};definitions=[];seen=set()
        for obj in data['deployments']+data['flink']:
            template=obj['spec'].get('template',obj['spec'].get('podTemplate',{}))
            for volume in template.get('spec',{}).get('volumes',[]):
                cm=volume.get('configMap',{});name=cm.get('name','')
                if not name.startswith('context-config-'):continue
                values=configs.get(name,{}).get('data',{})
                for item in cm.get('items',[]):
                    path=item.get('path');raw=values.get(item.get('key'),'')
                    if path not in ('ingestion.json','jobs.yaml','queries.yaml'):continue
                    identity=(obj['metadata']['name'],name,path)
                    if identity in seen:continue
                    seen.add(identity)
                    try:cfg=yaml.safe_load(raw)
                    except Exception:errors.append('invalid deployed configuration');continue
                    if not isinstance(cfg,dict):continue
                    if path=='ingestion.json':detail={'endpoints':[{k:e.get(k) for k in ('path','name','kind','topic','schema','description')} for e in cfg.get('endpoints',[])],'errorTopic':cfg.get('errorTopic')}
                    elif path=='jobs.yaml':detail={k:cfg.get(k) for k in ('name','sourceTopics','outputs','aggregations','watermarkDelaySeconds','idleSeconds','consumerGroup')}
                    else:detail={k:cfg.get(k) for k in ('queries','mutations','subscriptions','registeredTables')}
                    definitions.append({'service':obj['metadata']['name'],'configMap':name,'file':path,'definition':detail})
        for config in data['configs']:
            if config['metadata'].get('labels',{}).get('context-graph-inventory')=='true':
                try:definitions.append({'service':config['metadata']['name'],'configMap':config['metadata']['name'],'file':'bundle.json','definition':json.loads(config['data']['bundle.json'])})
                except (KeyError,ValueError):errors.append('invalid bundle inventory')
        services=[{'name':s['metadata']['name'],'type':s['spec'].get('type'),'ports':[{'name':p.get('name'),'port':p['port'],'protocol':p.get('protocol')} for p in s['spec'].get('ports',[])]} for s in data['services']]
        metrics={}
        try:
            targets=fetch('http://prometheus:9090/api/v1/targets')['data']['activeTargets']
            metrics['targets']=[{'job':t['labels'].get('job'),'instance':t['labels'].get('pod',t['labels'].get('instance')),'health':t['health'],'lastScrape':t.get('lastScrape'),'duration':t.get('lastScrapeDuration'),'error':bool(t.get('lastError'))} for t in targets]
            expressions={'requests':'sum(rate(context_http_requests_total{route!="health"}[5m]))','errors':'sum(rate(context_http_requests_total{status=~"5.."}[5m]))','latency':'histogram_quantile(0.95,sum by(le)(rate(context_http_request_duration_seconds_bucket{route!="health"}[5m])))','records':'sum(rate(flink_taskmanager_job_task_numRecordsIn[5m]))'}
            for key,expression in expressions.items():
                r=fetch('http://prometheus:9090/api/v1/query_range?'+urllib.parse.urlencode({'query':expression,'start':int(time.time())-900,'end':int(time.time()),'step':30}))
                metrics[key]=r['data']['result']
        except Exception:errors.append('metrics unavailable')
        connectors={}
        try:connectors=fetch('http://connectors:9406/status')
        except Exception:errors.append('connector status unavailable')
        return {'connectors':connectors,'namespace':NS,'observedAt':time.time(),'readOnly':True,'errors':errors,'workloads':workloads,'pods':pods,'services':services,'flink':jobs,'definitions':definitions,'metrics':metrics}

class Handler(BaseHTTPRequestHandler):
    def log_message(self,*args):pass
    def reply(self,status,value):
        REQUESTS.labels(str(status)).inc();body=json.dumps(value,allow_nan=False).encode()
        self.send_response(status);self.send_header('Content-Type','application/json');self.send_header('Cache-Control','no-store');self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
    def do_GET(self):
        if self.path=='/health/live':self.reply(200,{'status':'ok'});return
        if self.path!='/control/api/snapshot':self.reply(404,{'error':'not_found'});return
        if not SLOTS.acquire(False):self.reply(503,{'error':'busy'});return
        with DURATION.time():
            try:
                claims=self.server.auth.authenticate(self.headers.get('Authorization'));self.server.auth.check(claims)
                data=self.server.inventory.snapshot()
                self.server.auth.check(claims) # revocation/expiry during collection fails closed
                self.reply(200,data)
            except Denied as e:self.reply(e.status,{'error':'access_denied' if e.status<500 else 'authorization_unavailable'})
            except Exception:self.reply(503,{'error':'inventory_unavailable'})
            finally:SLOTS.release()
    def do_POST(self):self.reply(405,{'error':'read_only'})
    do_PUT=do_POST;do_PATCH=do_POST;do_DELETE=do_POST

if __name__=='__main__':
    start_http_server(9404)
    server=ThreadingHTTPServer(('0.0.0.0',8443),Handler);server.daemon_threads=True
    server.auth=Authorization();server.inventory=Inventory()
    context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);context.minimum_version=ssl.TLSVersion.TLSv1_2
    context.load_cert_chain(ROOT/'server.crt',ROOT/'server.key');server.socket=context.wrap_socket(server.socket,server_side=True)
    server.serve_forever()
