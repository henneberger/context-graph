#!/usr/bin/env python3
"""Provision local control-plane TLS and a PostgreSQL monitoring-only identity."""
import argparse,base64,datetime,json,secrets,subprocess
from pathlib import Path
from cryptography import x509
from cryptography.hazmat.primitives import hashes,serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID,ExtendedKeyUsageOID
p=argparse.ArgumentParser();p.add_argument('--context',required=True);args=p.parse_args()
root=Path('.runtime/security');out=root/'control-secrets.json'
if not out.exists():
 ca=x509.load_pem_x509_certificate((root/'ca.crt').read_bytes());cakey=serialization.load_pem_private_key((root/'ca.key').read_bytes(),None);key=rsa.generate_private_key(public_exponent=65537,key_size=2048);now=datetime.datetime.now(datetime.timezone.utc)
 cert=x509.CertificateBuilder().subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME,'control')])).issuer_name(ca.subject).public_key(key.public_key()).serial_number(x509.random_serial_number()).not_valid_before(now-datetime.timedelta(minutes=5)).not_valid_after(now+datetime.timedelta(days=90)).add_extension(x509.BasicConstraints(ca=False,path_length=None),True).add_extension(x509.SubjectKeyIdentifier.from_public_key(key.public_key()),False).add_extension(x509.AuthorityKeyIdentifier.from_issuer_public_key(cakey.public_key()),False).add_extension(x509.SubjectAlternativeName([x509.DNSName(n) for n in ('control','control.context-graph.svc.cluster.local','localhost')]),False).add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]),False).sign(cakey,hashes.SHA256())
 password=secrets.token_urlsafe(32);pw=root/'monitor-password';pw.write_text(password);pw.chmod(0o600)
 def secret(name,data):return {'apiVersion':'v1','kind':'Secret','metadata':{'name':name,'namespace':'context-graph'},'data':{k:base64.b64encode(v if isinstance(v,bytes) else v.encode()).decode() for k,v in data.items()}}
 items=[secret('control-security',{'server.crt':cert.public_bytes(serialization.Encoding.PEM),'server.key':key.private_bytes(serialization.Encoding.PEM,serialization.PrivateFormat.PKCS8,serialization.NoEncryption()),'ca.crt':(root/'ca.crt').read_bytes()}),secret('postgres-monitor',{'uri':f'postgresql://context_monitor:{password}@spicedb-postgres:5432/postgres?sslmode=verify-full&sslrootcert=/run/ca/ca.crt'})]
 out.write_text(json.dumps({'apiVersion':'v1','kind':'List','items':items}));out.chmod(0o600)
password=(root/'monitor-password').read_text()
sql="SELECT 'CREATE ROLE context_monitor LOGIN PASSWORD ''"+password+"''' WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname='context_monitor')\\gexec\nGRANT pg_monitor TO context_monitor;\nALTER ROLE context_monitor SET default_transaction_read_only=on;\n"
base=['kubectl','--context',args.context,'-n','context-graph']
subprocess.run(base+['exec','-i','spicedb-postgres-0','--','psql','-U','spicedb','-d','postgres','-v','ON_ERROR_STOP=1'],input=sql,text=True,check=True,stdout=subprocess.DEVNULL)
subprocess.run(base+['apply','-f',str(out)],check=True,stdout=subprocess.DEVNULL)
print('Control TLS and monitoring-only database credentials provisioned.')
