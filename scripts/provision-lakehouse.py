#!/usr/bin/env python3
"""Idempotent local TLS/service credentials and isolated Polaris PostgreSQL database."""
import base64,datetime,json,secrets,subprocess,os
from pathlib import Path
from cryptography import x509
from cryptography.hazmat.primitives import hashes,serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID,ExtendedKeyUsageOID
root=Path('.runtime/security');file=root/'lakehouse-secrets.json';creds_file=root/'lakehouse-credentials.json'
def private(path,data):path.write_text(data);path.chmod(0o600)
if not file.exists():
 ca=x509.load_pem_x509_certificate((root/'ca.crt').read_bytes());cakey=serialization.load_pem_private_key((root/'ca.key').read_bytes(),None)
 creds={n:secrets.token_urlsafe(32) for n in ['s3_root_key','s3_root_secret','polaris_root_secret','pg_password','media_secret','recovery_secret','warehouse_secret']}
 private(creds_file,json.dumps(creds))
 def cert(name):
  key=rsa.generate_private_key(public_exponent=65537,key_size=2048);now=datetime.datetime.now(datetime.timezone.utc)
  crt=x509.CertificateBuilder().subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME,name)])).issuer_name(ca.subject).public_key(key.public_key()).serial_number(x509.random_serial_number()).not_valid_before(now-datetime.timedelta(minutes=5)).not_valid_after(now+datetime.timedelta(days=90)).add_extension(x509.SubjectKeyIdentifier.from_public_key(key.public_key()),False).add_extension(x509.AuthorityKeyIdentifier.from_issuer_public_key(cakey.public_key()),False).add_extension(x509.BasicConstraints(ca=False,path_length=None),True).add_extension(x509.SubjectAlternativeName([x509.DNSName(x) for x in [name,name+'.context-graph.svc.cluster.local','localhost']]),False).add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]),False).sign(cakey,hashes.SHA256())
  return {'server.crt':crt.public_bytes(serialization.Encoding.PEM),'server.key':key.private_bytes(serialization.Encoding.PEM,serialization.PrivateFormat.PKCS8,serialization.NoEncryption()),'ca.crt':(root/'ca.crt').read_bytes()}
 def secret(name,data):return {'apiVersion':'v1','kind':'Secret','metadata':{'name':name,'namespace':'context-graph'},'data':{k:base64.b64encode(v.encode() if isinstance(v,str) else v).decode() for k,v in data.items()}}
 tls=cert('object-storage');tls['rustfs_cert.pem']=tls.pop('server.crt');tls['rustfs_key.pem']=tls.pop('server.key')
 items=[secret('object-storage-security',{**tls,'access-key':creds['s3_root_key'],'secret-key':creds['s3_root_secret']})]
 tls=cert('polaris');sign=rsa.generate_private_key(public_exponent=65537,key_size=2048)
 tls.update({'signing.key':sign.private_bytes(serialization.Encoding.PEM,serialization.PrivateFormat.PKCS8,serialization.NoEncryption()),'signing.pub':sign.public_key().public_bytes(serialization.Encoding.PEM,serialization.PublicFormat.SubjectPublicKeyInfo),'kafka.truststore.p12':(root/'kafka.truststore.p12').read_bytes(),'pg-password':creds['pg_password'],'root-secret':creds['polaris_root_secret'],'bootstrap-credential':'POLARIS,root,'+creds['polaris_root_secret']})
 items.append(secret('polaris-security',tls))
 for role in ['media','recovery','warehouse']:
  items.append(secret('object-storage-'+role,{'access-key':'context-'+role,'secret-key':creds[role+'_secret']}))
 private(file,json.dumps({'apiVersion':'v1','kind':'List','items':items}))
creds=json.loads(creds_file.read_text())
# Password alphabet is generated URL-safe; no credentials appear in argv or output.
sql="SELECT 'CREATE ROLE polaris LOGIN PASSWORD ''"+creds['pg_password']+"''' WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname='polaris')\\gexec\nSELECT 'CREATE DATABASE polaris OWNER polaris' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname='polaris')\\gexec\nREVOKE ALL ON DATABASE polaris FROM PUBLIC;\n"
subprocess.run(['kubectl','--context',os.environ.get('KUBE_CONTEXT','docker-desktop'),'-n','context-graph','exec','-i','spicedb-postgres-0','--','psql','-v','ON_ERROR_STOP=1','-U','spicedb','-d','postgres'],input=sql,text=True,check=True,stdout=subprocess.DEVNULL)
subprocess.run(['kubectl','--context',os.environ.get('KUBE_CONTEXT','docker-desktop'),'apply','-f',str(file)],check=True,stdout=subprocess.DEVNULL)
print('Lakehouse credentials, TLS and database prepared; credentials remain private.')
