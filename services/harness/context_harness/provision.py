"""Trusted provisioning: private credentials, exact topic grants, namespace-scoped catalog roles."""
import base64,datetime as dt,json,secrets,subprocess,sys
from pathlib import Path
import requests,yaml
from cryptography import x509
from cryptography.hazmat.primitives import hashes,serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.serialization import pkcs12
from cryptography.x509.oid import NameOID,ExtendedKeyUsageOID
from .compiler import ROOT,Invalid
from .release import kubectl,meta,NS
PRIVATE=ROOT/'.runtime/security'
def secret(name,values):return {'apiVersion':'v1','kind':'Secret','metadata':meta(name),'data':{k:base64.b64encode(v if isinstance(v,bytes) else v.encode()).decode() for k,v in values.items()}}
def apply_secret(context,name,values):kubectl(context,'apply','-f','-',input=json.dumps(secret(name,values)),stdout=subprocess.DEVNULL)
def private_json(path,value):path.parent.mkdir(parents=True,exist_ok=True);path.write_text(json.dumps(value));path.chmod(0o600)
def tls(name, stable):
    ca=x509.load_pem_x509_certificate((PRIVATE/'ca.crt').read_bytes());cakey=serialization.load_pem_private_key((PRIVATE/'ca.key').read_bytes(),None)
    key=rsa.generate_private_key(public_exponent=65537,key_size=2048);now=dt.datetime.now(dt.timezone.utc);password=secrets.token_urlsafe(24)
    cert=x509.CertificateBuilder().subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME,name)])).issuer_name(ca.subject).public_key(key.public_key()).serial_number(x509.random_serial_number()).not_valid_before(now-dt.timedelta(minutes=5)).not_valid_after(now+dt.timedelta(days=90)).add_extension(x509.BasicConstraints(ca=False,path_length=None),True).add_extension(x509.SubjectAlternativeName([x509.DNSName(n) for n in (name,name+'.'+NS+'.svc.cluster.local',stable,stable+'.'+NS+'.svc.cluster.local','localhost')]),False).add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]),False).sign(cakey,hashes.SHA256())
    return {'server.p12':pkcs12.serialize_key_and_certificates(name.encode(),key,cert,[ca],serialization.BestAvailableEncryption(password.encode())),'tls-password':password,'ca.crt':(PRIVATE/'ca.crt').read_bytes()}

def provision(b,context):
    release=b['release'];directory=PRIVATE/'bundles'/release;directory.mkdir(parents=True,exist_ok=True)
    passwords=directory/'passwords.json'
    if not passwords.exists():private_json(passwords,{r:secrets.token_urlsafe(32) for r in ('ingestion','processor','query')})
    credentials=json.loads(passwords.read_text())
    for role,pw in credentials.items():
        user=release+'-'+role
        props='\n'.join(['security.protocol=SASL_SSL','sasl.mechanism=SCRAM-SHA-512',f'sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="{user}" password="{pw}";',
          'ssl.truststore.type=PKCS12','ssl.truststore.location=/app/secrets/kafka.truststore.p12','ssl.truststore.password=changeit','ssl.endpoint.identification.algorithm=https',''])
        values={'kafka.properties':props,'kafka.truststore.p12':(PRIVATE/'kafka.truststore.p12').read_bytes()}
        if role!='processor':values.update(tls(user,'cg-'+b['name']+'-'+role))
        apply_secret(context,user,values)
    # Extend authentication without changing existing PLAIN service identities.
    state=json.loads(subprocess.check_output(['kubectl','--context',context,'-n',NS,'get','statefulset','secure-kafka','-o','json']))
    container=state['spec']['template']['spec']['containers'][0];env={v['name']:v.get('value') for v in container['env']}
    if 'SCRAM-SHA-512' not in env.get('KAFKA_SASL_ENABLED_MECHANISMS',''):
        kubectl(context,'set','env','statefulset/secure-kafka','KAFKA_SASL_ENABLED_MECHANISMS=PLAIN,SCRAM-SHA-512',
          'KAFKA_LISTENER_NAME_CLIENT_SCRAM___SHA___512_SASL_JAAS_CONFIG=org.apache.kafka.common.security.scram.ScramLoginModule required;',stdout=subprocess.DEVNULL)
        kubectl(context,'rollout','status','statefulset/secure-kafka','--timeout=300s')
    job_name=release+'-topics';lines=['set -euo pipefail','K=/opt/kafka/bin',
      'acl() { "$K/kafka-acls.sh" --bootstrap-server secure-kafka:9092 --command-config /app/secrets/kafka.properties --add "$@" >/dev/null; }']
    for role in credentials:
        lines.append('"$K/kafka-configs.sh" --bootstrap-server secure-kafka:9092 --command-config /app/secrets/kafka.properties --alter --entity-type users --entity-name '+release+'-'+role+' --add-config "SCRAM-SHA-512=[iterations=8192,password=$'+role.upper()+']" >/dev/null')
    output_topics=[v['topic'] for j in b['jobs'] for v in j['views']]
    for topic in list(b['inputs'].values())+output_topics+[b['errorTopic']]:
        lines.append('"$K/kafka-topics.sh" --bootstrap-server secure-kafka:9092 --command-config /app/secrets/kafka.properties --create --if-not-exists --topic '+topic+' --partitions 3 --replication-factor 3 --config min.insync.replicas=2 --config retention.ms=604800000 >/dev/null')
        if topic in b['inputs'].values():grants=[('ingestion','Write'),('processor','Read')]
        elif topic==b['errorTopic']:grants=[('ingestion','Write'),('processor','Write')]
        else:grants=[('processor','Write'),('query','Read')]
        for role,operation in grants:lines.append(f'acl --allow-principal User:{release}-{role} --operation {operation} --operation Describe --topic {topic}')
    for role in ('ingestion','processor'):lines.append(f'acl --allow-principal User:{release}-{role} --operation IdempotentWrite --cluster')
    lines.extend([f'acl --allow-principal User:{release}-processor --operation Read --group {release}- --resource-pattern-type prefixed',
      f'acl --allow-principal User:{release}-processor --operation Write --operation Describe --transactional-id {release}- --resource-pattern-type prefixed',
      f'acl --allow-principal User:{release}-query --operation Read --group context-query- --resource-pattern-type prefixed'])
    apply_secret(context,release+'-provision',{r.upper():v for r,v in credentials.items()})
    job={'apiVersion':'batch/v1','kind':'Job','metadata':meta(job_name),'spec':{'backoffLimit':2,'ttlSecondsAfterFinished':86400,'template':{
      'metadata':{'labels':{'app':'security-provisioner'}},'spec':{'automountServiceAccountToken':False,'restartPolicy':'Never','securityContext':{'fsGroup':1000},
      'containers':[{'name':'topics','image':'apache/kafka:4.1.1','command':['/bin/bash','-ec'],'args':['\n'.join(lines)],'envFrom':[{'secretRef':{'name':release+'-provision'}}],
        'volumeMounts':[{'name':'security','mountPath':'/app/secrets','readOnly':True}],'resources':{'requests':{'cpu':'100m','memory':'128Mi'},'limits':{'memory':'384Mi'}}}],
      'volumes':[{'name':'security','secret':{'secretName':'security-admin','defaultMode':288}}]}}}}
    existing=kubectl(context,'get','job',job_name,'--ignore-not-found','-o','name',capture_output=True).stdout.strip()
    if not existing:kubectl(context,'apply','-f','-',input=yaml.safe_dump(job),stdout=subprocess.DEVNULL)
    kubectl(context,'wait','--for=condition=complete','job/'+job_name,'--timeout=180s')
    # The completed Job no longer needs plaintext SCRAM passwords mounted in its pod.
    kubectl(context,'delete','secret',release+'-provision','--ignore-not-found',stdout=subprocess.DEVNULL)
    catalog(b,context,directory)

def catalog(b,context,directory):
    sys.path.insert(0,str(ROOT/'scripts'));from kube_forward import service_forward
    with service_forward(context,'polaris',8443) as address:
        s=requests.Session();s.verify=str(PRIVATE/'ca.crt');base='https://'+address
        creds=json.loads((PRIVATE/'lakehouse-credentials.json').read_text())
        r=s.post(base+'/api/catalog/v1/oauth/tokens',data={'grant_type':'client_credentials','scope':'PRINCIPAL_ROLE:ALL','client_id':'root','client_secret':creds['polaris_root_secret']},timeout=20);r.raise_for_status();s.headers['Authorization']='Bearer '+r.json()['access_token']
        def request(method,path,data=None,exists=False):
            result=s.request(method,base+path,json=data,timeout=30)
            if exists and result.status_code==409:return None
            if result.status_code>=400:raise RuntimeError(f'Catalog provisioning failed: {method} {path}: {result.status_code}')
            return result.json() if result.content else None
        ns=b['namespace'];release=b['release'];m='/api/management/v1';catalog=m+'/catalogs/context'
        request('POST','/api/catalog/v1/context/namespaces',{'namespace':[ns]},True)
        for role in ('processor','query'):
            name=release+'-'+role;stored=directory/('polaris-'+role+'.json')
            if not stored.exists():private_json(stored,request('POST',m+'/principals',{'principal':{'name':name},'credentialRotationRequired':False}))
            request('POST',m+'/principal-roles',{'principalRole':{'name':name}},True);request('POST',catalog+'/catalog-roles',{'catalogRole':{'name':name}},True)
            privileges=['NAMESPACE_READ_PROPERTIES','TABLE_LIST','TABLE_READ_PROPERTIES','TABLE_READ_DATA']+(['TABLE_CREATE','TABLE_WRITE_DATA'] if role=='processor' else [])
            for privilege in privileges:request('PUT',catalog+'/catalog-roles/'+name+'/grants',{'type':'namespace','namespace':[ns],'privilege':privilege})
            request('PUT',m+'/principal-roles/'+name+'/catalog-roles/context',{'catalogRole':{'name':name}})
            request('PUT',m+'/principals/'+name+'/principal-roles',{'principalRole':{'name':name}})
            credential=json.loads(stored.read_text())['credentials']
            apply_secret(context,release+'-catalog-'+role,{'client-id':credential['clientId'],'client-secret':credential['clientSecret'],'credential':credential['clientId']+':'+credential['clientSecret']})
        for view,description in b['views'].items():
            schema=description['schema']
            request('POST','/api/catalog/v1/context/namespaces/'+ns+'/tables',{'name':view,'schema':schema,
                'partition-spec':{'spec-id':0,'fields':[{'source-id':next(f['id'] for f in schema['fields'] if f['name']=='workspace_id'),'field-id':1000,'name':'workspace_id','transform':'identity'}]},
                'properties':{'format-version':'2','write.target-file-size-bytes':'134217728'}},True)
