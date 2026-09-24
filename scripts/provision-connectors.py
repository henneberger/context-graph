#!/usr/bin/env python3
"""Prepare private connector/search/Temporal TLS and scoped service identities."""
import argparse,base64,datetime as dt,hashlib,json,secrets,subprocess
from pathlib import Path
from cryptography import x509
from cryptography.hazmat.primitives import hashes,serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID,ExtendedKeyUsageOID
p=argparse.ArgumentParser();p.add_argument('--context',default='docker-desktop');a=p.parse_args()
root=Path('.runtime/security');raw=root/'connector-credentials.json';creds=json.loads(raw.read_text());ca=x509.load_pem_x509_certificate((root/'ca.crt').read_bytes());cakey=serialization.load_pem_private_key((root/'ca.key').read_bytes(),None)
def private(p,s):p.write_text(s);p.chmod(0o600)
def cert(name):
 file=root/(name+'-tls.json')
 if file.exists():return {k:base64.b64decode(v) for k,v in json.loads(file.read_text()).items()}
 key=rsa.generate_private_key(public_exponent=65537,key_size=2048);now=dt.datetime.now(dt.timezone.utc)
 c=x509.CertificateBuilder().subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME,name)])).issuer_name(ca.subject).public_key(key.public_key()).serial_number(x509.random_serial_number()).not_valid_before(now-dt.timedelta(minutes=5)).not_valid_after(now+dt.timedelta(days=90)).add_extension(x509.BasicConstraints(ca=False,path_length=None),True).add_extension(x509.SubjectKeyIdentifier.from_public_key(key.public_key()),False).add_extension(x509.AuthorityKeyIdentifier.from_issuer_public_key(cakey.public_key()),False).add_extension(x509.SubjectAlternativeName([x509.DNSName(n) for n in (name,name+'.context-graph.svc.cluster.local','localhost')]),False).add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH,ExtendedKeyUsageOID.CLIENT_AUTH]),False).sign(cakey,hashes.SHA256())
 data={'server.crt':c.public_bytes(serialization.Encoding.PEM),'server.key':key.private_bytes(serialization.Encoding.PEM,serialization.PrivateFormat.PKCS8,serialization.NoEncryption()),'ca.crt':(root/'ca.crt').read_bytes()};private(file,json.dumps({k:base64.b64encode(v).decode() for k,v in data.items()}));return data
def secret(name,data):return {'apiVersion':'v1','kind':'Secret','metadata':{'name':name,'namespace':'context-graph'},'data':{k:base64.b64encode(v if isinstance(v,bytes) else v.encode()).decode() for k,v in data.items()}}
creds.setdefault('ingestion_password',secrets.token_urlsafe(32));creds.setdefault('database_password',secrets.token_urlsafe(32))
# The authenticated fine-grained credential was verified; invalid classic token is never mounted.
if not creds.get('github_token'):creds['github_token']=creds['github_tokens'][1]
private(raw,json.dumps(creds))
base=['kubectl','--context',a.context,'-n','context-graph']
pw=creds['database_password'];sql="SELECT 'CREATE ROLE connectors LOGIN PASSWORD ''"+pw+"''' WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname='connectors')\\gexec\nSELECT 'CREATE DATABASE connectors OWNER connectors' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname='connectors')\\gexec\nREVOKE ALL ON DATABASE connectors FROM PUBLIC;\n"
subprocess.run(base+['exec','-i','spicedb-postgres-0','--','psql','-U','spicedb','-d','postgres','-v','ON_ERROR_STOP=1'],input=sql,text=True,check=True,stdout=subprocess.DEVNULL)
identity=json.loads(subprocess.check_output(base+['get','secret','identity-security','-o','json']));users=json.loads(base64.b64decode(identity['data']['users.json']));salt=secrets.token_bytes(16);users['connector']={'subject':'connector','salt':salt.hex(),'hash':hashlib.scrypt(creds['ingestion_password'].encode(),salt=salt,n=16384,r=8,p=1).hex()};identity['data']['users.json']=base64.b64encode(json.dumps(users).encode()).decode()
subprocess.run(base+['patch','secret','identity-security','--type=merge','--patch-file','/dev/stdin'],input=json.dumps({'data':{'users.json':identity['data']['users.json']}}),text=True,check=True,stdout=subprocess.DEVNULL)
# Keep fresh deployments consistent with this local issuer credential.
bundle_path=root/'secrets.json';bundle=json.loads(bundle_path.read_text())
for item in bundle['items']:
 if item['metadata']['name']=='identity-security':item['data']['users.json']=identity['data']['users.json']
private(bundle_path,json.dumps(bundle))
workercreds={k:creds[k] for k in ('github_token','slack_bot_token','ingestion_password')}
items=[secret('connectors-security',{**cert('connectors'),'credentials.json':json.dumps(workercreds),'spicedb-token':(root/'spicedb-token').read_text().strip(),'database-uri':f'postgresql://connectors:{pw}@spicedb-postgres:5432/connectors?sslmode=verify-full&sslrootcert=/run/connectors/ca.crt'}),secret('temporal-security',{**cert('temporal'),'server.pem':cert('temporal')['server.crt']+cert('temporal')['server.key']}),secret('search-security',{**cert('search-api'),'deepseek-key':creds['deepseek_api_key']})]
file=root/'search-secrets.json';private(file,json.dumps({'apiVersion':'v1','kind':'List','items':items}));subprocess.run(base+['apply','-f',str(file)],check=True,stdout=subprocess.DEVNULL)
print('Scoped connector identities, database and TLS provisioned. No credentials printed.')
