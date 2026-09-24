"""Source-native ACLs and resumable, idempotent document ingestion. No credentials in histories."""
import base64,datetime as dt,hashlib,json,os,time,uuid
from pathlib import Path
from urllib.parse import urlparse
import psycopg,requests,yaml
from psycopg.types.json import Jsonb
from prometheus_client import Counter,Gauge
from temporalio import activity
from temporalio.exceptions import ApplicationError

ROOT=Path(os.environ.get('CONNECTOR_SECRETS','/run/connectors'))
CONFIG=Path(os.environ.get('CONNECTOR_CONFIG','/app/config/connectors/sources.yaml'))
REQUESTS=Counter('context_connector_provider_requests_total','Provider requests',['provider','status'])
DOCS=Counter('context_connector_documents_total','Acknowledged documents',['provider','operation'])
FAILURES=Counter('context_connector_failures_total','Source failures',['provider','reason'])
LAST_SUCCESS=Gauge('context_connector_last_success_timestamp_seconds','Last completed source sync',['provider'])

def now():return dt.datetime.now(dt.timezone.utc).isoformat().replace('+00:00','Z')
def digest(s):return hashlib.sha256(s.encode()).hexdigest()
def beat():
    if activity.in_activity():activity.heartbeat()
def config():return yaml.safe_load(CONFIG.read_text())
def database():return psycopg.connect(os.environ['CONNECTOR_DATABASE_URI'],autocommit=True)
def initialize():
    with database() as db:
        db.execute('CREATE TABLE IF NOT EXISTS sources (id text PRIMARY KEY, provider text NOT NULL, external_id text NOT NULL, name text NOT NULL, metadata jsonb NOT NULL, status text NOT NULL DEFAULT \'discovered\', last_success timestamptz, acl_verified timestamptz, error text, documents bigint NOT NULL DEFAULT 0)')
        db.execute('CREATE TABLE IF NOT EXISTS documents (source_id text NOT NULL, id text NOT NULL, fingerprint text NOT NULL, payload jsonb NOT NULL, seen_run text NOT NULL, PRIMARY KEY(source_id,id))')
        db.execute('CREATE TABLE IF NOT EXISTS identities (id text PRIMARY KEY, provider text NOT NULL, display_name text, active boolean NOT NULL, updated_at timestamptz NOT NULL DEFAULT now())')

def credentials():return json.loads((ROOT/'credentials.json').read_text())

class ProviderError(Exception):
    def __init__(self,code,retry=0):self.code=code;self.retry=retry;super().__init__(code)

class API:
    def __init__(self,provider):
        self.provider=provider;self.session=requests.Session();creds=credentials()
        token=creds['github_token'] if provider=='github' else creds['slack_bot_token']
        self.session.headers.update({'Authorization':'Bearer '+token,'User-Agent':'ContextGraphConnector/1.0'})
        if provider=='github':self.session.headers.update({'Accept':'application/vnd.github+json','X-GitHub-Api-Version':'2022-11-28'})
    def get(self,path,params=None):
        beat();base='https://api.github.com/' if self.provider=='github' else 'https://slack.com/api/'
        if '://' in path or path.startswith('/') or '..' in path:raise ValueError('Invalid provider path')
        try:r=self.session.get(base+path,params=params,timeout=(5,25),allow_redirects=False)
        except requests.RequestException:raise ProviderError('network_error',30) from None
        REQUESTS.labels(self.provider,str(r.status_code)).inc();beat()
        if r.status_code==429 or r.status_code==403 and r.headers.get('X-RateLimit-Remaining')=='0':
            delay=int(r.headers.get('Retry-After',max(60,int(r.headers.get('X-RateLimit-Reset',int(time.time())+60))-int(time.time()))))
            raise ProviderError('rate_limited',min(max(1,delay),3600))
        if r.status_code!=200:raise ProviderError('http_'+str(r.status_code),30 if r.status_code>=500 else 0)
        data=r.json()
        if self.provider=='slack' and not data.get('ok'):raise ProviderError(data.get('error','slack_error'))
        return data,r
    def pages(self,path,params=None,key=None):
        values=dict(params or {});values['per_page' if self.provider=='github' else 'limit']=100
        page=1;seen=set()
        while True:
            if self.provider=='github':values['page']=page
            body,response=self.get(path,values)
            items=body if key is None else body.get(key,[])
            if not isinstance(items,list):raise ProviderError('invalid_page')
            yield from items
            if self.provider=='github':
                if not response.links.get('next'):break
                page+=1
            else:
                cursor=body.get('response_metadata',{}).get('next_cursor','')
                if not cursor:
                    if body.get('has_more'):raise ProviderError('incomplete_pagination')
                    break
                if cursor in seen:raise ProviderError('repeated_cursor')
                seen.add(cursor);values['cursor']=cursor

def spice(path,body):
    try:r=requests.post('https://spicedb:8443/v1/'+path,json=body,headers={'Authorization':'Bearer '+(ROOT/'spicedb-token').read_text().strip()},verify=str(ROOT/'ca.crt'),timeout=15)
    except requests.RequestException:raise ProviderError('authorization_unavailable',15) from None
    if r.status_code!=200:raise ProviderError('authorization_update_failed',15)
    return r

def relation(kind,identifier,rel,subject_type,subject_id,subject_relation=None,expires=None):
    item={'resource':{'objectType':kind,'objectId':identifier},'relation':rel,'subject':{'object':{'objectType':subject_type,'objectId':subject_id}}}
    if subject_relation:item['subject']['optionalRelation']=subject_relation
    if expires:item['optionalExpiresAt']=expires
    return item

def touch(items):
    for offset in range(0,len(items),100):spice('relationships/write',{'updates':[{'operation':'OPERATION_TOUCH','relationship':i} for i in items[offset:offset+100]]})

def register_identity(provider,identifier,name,active=True):
    canonical=provider+':'+identifier
    with database() as db:db.execute('INSERT INTO identities(id,provider,display_name,active) VALUES(%s,%s,%s,%s) ON CONFLICT(id) DO UPDATE SET display_name=excluded.display_name,active=excluded.active,updated_at=now()',(canonical,provider,name[:256],active))
    principal=provider+'-'+digest(canonical)
    cfg=config();accounts=[principal]+cfg.get('identity_links',{}).get(canonical,[]) if active else []
    # Existing login links are operator-owned; active source membership governs read grants.
    if accounts:touch([relation('external_identity',principal,'account','user',a) for a in accounts]+[relation('workspace',cfg['workspace'],'member','user',a) for a in accounts])
    return principal

def source_id(provider,external_id):return provider+'-'+digest(str(external_id))[:32]
def upsert_source(provider,external_id,name,metadata):
    identifier=source_id(provider,external_id)
    with database() as db:db.execute('INSERT INTO sources(id,provider,external_id,name,metadata) VALUES(%s,%s,%s,%s,%s) ON CONFLICT(id) DO UPDATE SET name=excluded.name,metadata=excluded.metadata',(identifier,provider,str(external_id),name,Jsonb(metadata)))
    workspace=config()['workspace'];entity='source-'+identifier;resource=digest(workspace+'\0'+entity)
    # Imported wildcard closes workspace-admin and local reader/writer bypasses.
    touch([relation('entity',resource,'workspace','workspace',workspace),relation('entity',resource,'imported','user','*'),relation('entity',resource,'source','source',identifier),relation('entity',resource,'writer','user','connector'),relation('workspace',workspace,'member','user','connector')])
    return identifier

def set_acl(identifier,principals,public=False):
    response=spice('relationships/read',{'consistency':{'fully_consistent':True},'relationshipFilter':{'resourceType':'source','optionalResourceId':identifier,'optionalRelation':'reader'}})
    old=[]
    for line in response.text.splitlines():
        if line.strip():
            rel=json.loads(line).get('result',{}).get('relationship')
            if rel:old.append(rel)
    expiry=(dt.datetime.now(dt.timezone.utc)+dt.timedelta(minutes=config()['acl_lease_minutes'])).isoformat().replace('+00:00','Z')
    desired=[relation('source',identifier,'reader','external_identity',p,'account',expiry) for p in sorted(set(principals))]
    if public:desired.append(relation('source',identifier,'reader','user','*',expires=expiry))
    def key(r):return json.dumps([r['subject']['object'],r['subject'].get('optionalRelation','')],sort_keys=True)
    keys={key(r) for r in desired}
    updates=[{'operation':'OPERATION_DELETE','relationship':r} for r in old if key(r) not in keys]+[{'operation':'OPERATION_TOUCH','relationship':r} for r in desired]
    # Refuse oversized ACLs rather than publish a partial grant set.
    if len(updates)>900:raise ProviderError('acl_too_large')
    if updates:spice('relationships/write',{'updates':updates})
    with database() as db:db.execute('UPDATE sources SET acl_verified=now() WHERE id=%s',(identifier,))

def source_row(identifier):
    with database() as db:
        row=db.execute('SELECT provider,external_id,name,metadata FROM sources WHERE id=%s',(identifier,)).fetchone()
        if not row:raise ProviderError('source_missing')
        return dict(zip(('provider','external_id','name','metadata'),row))

def refresh_one(identifier):
    source=source_row(identifier);provider=source['provider'];api=API(provider)
    try:
        if provider=='github':
            repo,_=api.get('repos/'+source['name']);principals=[]
            # Public repository content is public to authenticated knowledge-workspace members.
            if repo['private']:
                # The authenticated token owner is a verified reader even when collaborator enumeration is denied.
                me,_=api.get('user');principals.append(register_identity('github',str(me['id']),me['login']))
                try:
                    for u in api.pages('repos/'+source['name']+'/collaborators'):principals.append(register_identity('github',str(u['id']),u['login']))
                except ProviderError as e:
                    if e.code not in ('http_403','http_404'):raise
                    # Undergrant: no guessed org/team membership.
            set_acl(identifier,principals,public=not repo['private'])
        else:
            channel,_=api.get('conversations.info',{'channel':source['external_id']});channel=channel['channel']
            users=list(api.pages('users.list',key='members'));active={u['id']:u for u in users if not u.get('deleted') and not u.get('is_bot') and u['id']!='USLACKBOT'}
            team=source['metadata']['team'];members=list(api.pages('conversations.members',{'channel':source['external_id']},key='members'))
            # Public channels: conservatively grant channel members, not every directory user.
            principals=[register_identity('slack',team+':'+uid,active[uid].get('real_name',active[uid].get('name',''))) for uid in members if uid in active]
            set_acl(identifier,principals)
        return True
    except ProviderError as e:
        # Any failed ACL refresh closes the old grants immediately when SpiceDB is reachable.
        try:set_acl(identifier,[])
        except ProviderError:pass
        with database() as db:db.execute('UPDATE sources SET error=%s,status=%s WHERE id=%s',(e.code,'acl_unavailable',identifier))
        raise

@activity.defn
def discover_sources(provider:str):
    api=API(provider);ids=[];cfg=config()
    try:
        if provider=='github':
            repos=cfg['github']['repositories']
            if not isinstance(repos,list) or not repos:raise ProviderError('repository_allowlist_required')
            me,_=api.get('user');register_identity('github',str(me['id']),me['login'])
            for name in repos:
                if len(name.split('/'))!=2 or any(c not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-._/' for c in name):raise ProviderError('invalid_repository')
                repo,_=api.get('repos/'+name);ids.append(upsert_source(provider,str(repo['id']),repo['full_name'],{'private':repo['private'],'branch':repo['default_branch']}))
        else:
            auth,_=api.get('auth.test');team=auth['team_id']
            for user in api.pages('users.list',key='members'):register_identity('slack',team+':'+user['id'],user.get('real_name',user.get('name','')),not user.get('deleted',False))
            for c in api.pages('conversations.list',{'types':'public_channel,private_channel','exclude_archived':'false'},key='channels'):
                ids.append(upsert_source(provider,c['id'],c['name'],{'private':c['is_private'],'member':c.get('is_member',False),'team':team}))
        # Removed/inaccessible/config-excluded sources lose their grants as well.
        with database() as db:old=[r[0] for r in db.execute('SELECT id FROM sources WHERE provider=%s',(provider,))]
        for identifier in set(old)-set(ids):set_acl(identifier,[])
        return ids
    except ProviderError as e:raise ApplicationError(e.code,type=e.code,next_retry_delay=dt.timedelta(seconds=e.retry or 60)) from None

@activity.defn
def refresh_permissions(provider:str):
    with database() as db:ids=[r[0] for r in db.execute('SELECT id FROM sources WHERE provider=%s',(provider,))]
    passed=0
    for identifier in ids:
        beat()
        source=source_row(identifier)
        if provider=='github' and source['name'].lower() not in {n.lower() for n in config()['github']['repositories']}:set_acl(identifier,[]);continue
        try:refresh_one(identifier);passed+=1
        except ProviderError as e:FAILURES.labels(provider,e.code).inc()
    return {'sources':len(ids),'refreshed':passed}

class Publisher:
    def __init__(self):self.token=None;self.expires=0
    def publish(self,provider,payload):
        if self.expires<time.time()+30:
            c=credentials();r=requests.post('https://identity:8443/token',json={'username':'connector','password':c['ingestion_password']},verify=str(ROOT/'ca.crt'),timeout=15)
            if r.status_code!=200:raise ProviderError('ingestion_identity_unavailable',30)
            self.token=r.json()['access_token'];self.expires=time.time()+r.json()['expires_in']
        r=requests.post('https://ingestion:8080/ingest/'+provider,json=payload,headers={'Authorization':'Bearer '+self.token,'X-Workspace-Id':config()['workspace']},verify=str(ROOT/'ca.crt'),timeout=40)
        if r.status_code!=202:raise ProviderError('ingestion_rejected_'+str(r.status_code),30)
        DOCS.labels(provider,'delete' if payload['deleted'] else 'upsert').inc()

def make_document(source,identifier,kind,doc_id,title,text,url,updated,author=''):
    host=urlparse(url).hostname or ''
    if not url.startswith('https://') or not (host=='github.com' if source['provider']=='github' else host.endswith('.slack.com')):raise ProviderError('invalid_citation_url')
    return {'entityId':'source-'+identifier,'documentId':doc_id,'source':source['provider'],'sourceId':identifier,'documentType':kind,'title':str(title)[:1000],'text':str(text or '')[:100000],'url':url,'author':str(author)[:256],'container':source['name'][:256],'updatedAt':updated,'observedAt':now(),'eventTime':now(),'deleted':False}

def github_documents(api,source,identifier):
    root='repos/'+source['name'];cutoff=config().get('initial_backfill_days');params={'state':'all','sort':'updated','direction':'desc'}
    if cutoff:params['since']=(dt.datetime.now(dt.timezone.utc)-dt.timedelta(days=cutoff)).isoformat()
    if 'readme' in config()['github'].get('kinds',[]):
        try:readme,_=api.get(root+'/readme')
        except ProviderError as error:
            if error.code!='http_404':raise
        else:
            if readme.get('encoding')!='base64' or readme.get('size',0)>100000:raise ProviderError('unsupported_readme')
            body=base64.b64decode(readme['content']).decode('utf-8')
            changes,_=api.get(root+'/commits',{'path':readme['path'],'per_page':1})
            if not changes:raise ProviderError('readme_history_unavailable')
            updated=changes[0]['commit']['committer']['date']
            yield make_document(source,identifier,'document','github:'+source['external_id']+':readme',source['name']+' — README',body,readme['html_url'],updated,source['name'].split('/')[0])
    for issue in api.pages(root+'/issues',params):
        user=issue.get('user') or {};author=user.get('login','')
        if user.get('id'):register_identity('github',str(user['id']),author)
        kind='pull_request' if 'pull_request' in issue else 'issue'
        text='Status: '+issue['state']+'\nLabels: '+', '.join(x['name'] for x in issue.get('labels',[]))+'\n\n'+(issue.get('body') or '')
        if issue.get('comments'):
            for comment in api.pages(root+'/issues/'+str(issue['number'])+'/comments'):
                text+='\n\n'+(comment.get('user') or {}).get('login','')+': '+(comment.get('body') or '')
        # Pull request review comments are separate from issue comments.
        if kind=='pull_request':
            details,_=api.get(root+'/pulls/'+str(issue['number']))
            text='Pull request status: '+('merged' if details.get('merged') else details['state'])+'\n'+text
            for comment in api.pages(root+'/pulls/'+str(issue['number'])+'/comments'):text+='\n\n'+(comment.get('body') or '')
        yield make_document(source,identifier,kind,'github:'+source['external_id']+':'+kind+':'+str(issue['number']),issue['title'],text,issue['html_url'],issue['updated_at'],author)
    params={}
    if cutoff:params['since']=(dt.datetime.now(dt.timezone.utc)-dt.timedelta(days=cutoff)).isoformat()
    for commit in api.pages(root+'/commits',params):
        author=commit.get('author') or {};body=commit['commit'];msg=body['message']
        if author.get('id'):register_identity('github',str(author['id']),author.get('login',''))
        yield make_document(source,identifier,'commit','github:'+source['external_id']+':commit:'+commit['sha'],msg.split('\n')[0],msg,commit['html_url'],body['committer']['date'],author.get('login',body.get('author',{}).get('name','')))

def slack_documents(api,source,identifier):
    channel=source['external_id'];team=source['metadata']['team'];seen=set();params={'channel':channel};cutoff=config().get('initial_backfill_days')
    if cutoff:params['oldest']=str(time.time()-cutoff*86400)
    with database() as db:names={row[0].split(':')[-1]:row[1] for row in db.execute("SELECT id,display_name FROM identities WHERE provider='slack'")}
    def convert(message):
        ts=message['ts'];updated=message.get('edited',{}).get('ts',ts);text=message.get('text','')
        # Durable official deep link; no permalink API call per document.
        url=f'https://app.slack.com/archives/{channel}/p{ts.replace(".","")}'
        return make_document(source,identifier,'message',f'slack:{team}:{channel}:{ts}',text.split('\n')[0] or '#'+source['name'],text,url,dt.datetime.fromtimestamp(float(updated),dt.timezone.utc).isoformat(),names.get(message.get('user',''),message.get('user',message.get('bot_id',''))))
    for message in api.pages('conversations.history',params,key='messages'):
        if message.get('subtype') in ('message_deleted','channel_join','channel_leave'):continue
        ts=message['ts'];seen.add(ts);yield convert(message)
        if message.get('reply_count',0)>0 and config()['slack'].get('include_threads'):
            # A missing replies scope is surfaced as incomplete, never a successful full sync.
            for reply in api.pages('conversations.replies',{'channel':channel,'ts':ts},key='messages'):
                if reply['ts'] not in seen:seen.add(reply['ts']);yield convert(reply)

@activity.defn
def sync_source(identifier:str):
    source=source_row(identifier);provider=source['provider'];run=uuid.uuid4().hex;publisher=Publisher();count=0
    try:
        if provider=='github' and source['name'].lower() not in {n.lower() for n in config()['github']['repositories']}:set_acl(identifier,[]);return {'excluded':True}
        refresh_one(identifier)
        with database() as db:db.execute('UPDATE sources SET status=\'syncing\',error=NULL WHERE id=%s',(identifier,))
        api=API(provider);stream=github_documents(api,source,identifier) if provider=='github' else slack_documents(api,source,identifier)
        for payload in stream:
            beat();stable={k:v for k,v in payload.items() if k not in ('observedAt','eventTime')};fingerprint=digest(json.dumps(stable,sort_keys=True))
            with database() as db:previous=db.execute('SELECT fingerprint FROM documents WHERE source_id=%s AND id=%s',(identifier,payload['documentId'])).fetchone()
            if not previous or previous[0]!=fingerprint:publisher.publish(provider,payload)
            # Ack first, cursor/hash second. Retry can duplicate events; document projection deduplicates.
            with database() as db:db.execute('INSERT INTO documents(source_id,id,fingerprint,payload,seen_run) VALUES(%s,%s,%s,%s,%s) ON CONFLICT(source_id,id) DO UPDATE SET fingerprint=excluded.fingerprint,payload=excluded.payload,seen_run=excluded.seen_run',(identifier,payload['documentId'],fingerprint,Jsonb({**payload,"text":"","title":""}),run))
            count+=1
        # Only a fully paginated successful snapshot may mark missing documents deleted.
        with database() as db:missing=db.execute('SELECT id,payload FROM documents WHERE source_id=%s AND seen_run<>%s',(identifier,run)).fetchall()
        for doc_id,payload in missing:
            beat();payload.update(deleted=True,text='',title='Removed document',observedAt=now(),eventTime=now());publisher.publish(provider,payload)
            with database() as db:db.execute('DELETE FROM documents WHERE source_id=%s AND id=%s',(identifier,doc_id))
        with database() as db:db.execute('UPDATE sources SET status=\'ready\',error=NULL,last_success=now(),documents=%s WHERE id=%s',(count,identifier))
        LAST_SUCCESS.labels(provider).set_to_current_time();return {'documents':count,'deleted':len(missing)}
    except ProviderError as e:
        FAILURES.labels(provider,e.code).inc()
        # Deny stale data after read permission loss or an incomplete snapshot.
        try:set_acl(identifier,[])
        except ProviderError:pass
        with database() as db:db.execute('UPDATE sources SET status=\'incomplete\',error=%s WHERE id=%s',(e.code,identifier))
        raise ApplicationError(e.code,type=e.code,non_retryable=e.retry==0,next_retry_delay=dt.timedelta(seconds=e.retry) if e.retry else None) from None
