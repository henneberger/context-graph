#!/usr/bin/env python3
"""Create local credentials and TLS material; emit Kubernetes Secrets without printing secrets."""
import argparse,base64,datetime,hashlib,json,os,secrets,subprocess,tempfile
from pathlib import Path
from cryptography import x509
from cryptography.hazmat.primitives import hashes,serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.serialization import pkcs12
from cryptography.x509.oid import NameOID

DIR=Path('.runtime/security');NS='context-graph'
def write(name,data):
    path=DIR/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(data.encode() if isinstance(data,str) else data);path.chmod(0o600);return path

def make_secret(name,files):
    return {'apiVersion':'v1','kind':'Secret','metadata':{'name':name,'namespace':NS},'type':'Opaque','data':{k:base64.b64encode(v.encode() if isinstance(v,str) else v).decode() for k,v in files.items()}}

def create():
    if (DIR/'secrets.json').exists():return
    DIR.mkdir(parents=True,exist_ok=True);DIR.chmod(0o700)
    now=datetime.datetime.now(datetime.timezone.utc);ca_key=rsa.generate_private_key(public_exponent=65537,key_size=3072)
    ca_name=x509.Name([x509.NameAttribute(NameOID.COMMON_NAME,'Context Graph local CA')])
    ca=x509.CertificateBuilder().subject_name(ca_name).issuer_name(ca_name).public_key(ca_key.public_key()).serial_number(x509.random_serial_number()).not_valid_before(now-datetime.timedelta(minutes=5)).not_valid_after(now+datetime.timedelta(days=365)).add_extension(x509.BasicConstraints(ca=True,path_length=0),critical=True).add_extension(x509.SubjectKeyIdentifier.from_public_key(ca_key.public_key()),False).add_extension(x509.KeyUsage(False,False,False,False,False,True,True,False,False),True).sign(ca_key,hashes.SHA256())
    ca_pem=ca.public_bytes(serialization.Encoding.PEM);write('ca.crt',ca_pem);write('ca.key',ca_key.private_bytes(serialization.Encoding.PEM,serialization.PrivateFormat.PKCS8,serialization.NoEncryption()))
    trust=pkcs12.serialize_java_truststore([pkcs12.PKCS12Certificate(ca,b'context-ca')],serialization.BestAvailableEncryption(b'changeit'))
    def certificate(name,names):
        key=rsa.generate_private_key(public_exponent=65537,key_size=2048)
        cert=x509.CertificateBuilder().subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME,name)])).issuer_name(ca.subject).public_key(key.public_key()).serial_number(x509.random_serial_number()).not_valid_before(now-datetime.timedelta(minutes=5)).not_valid_after(now+datetime.timedelta(days=90)).add_extension(x509.SubjectAlternativeName([x509.DNSName(n) for n in names]),critical=False).add_extension(x509.ExtendedKeyUsage([x509.oid.ExtendedKeyUsageOID.SERVER_AUTH,x509.oid.ExtendedKeyUsageOID.CLIENT_AUTH]),critical=False).add_extension(x509.BasicConstraints(ca=False,path_length=None),True).add_extension(x509.SubjectKeyIdentifier.from_public_key(key.public_key()),False).add_extension(x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()),False).sign(ca_key,hashes.SHA256())
        password=secrets.token_urlsafe(24)
        return {'server.key':key.private_bytes(serialization.Encoding.PEM,serialization.PrivateFormat.PKCS8,serialization.NoEncryption()),'server.crt':cert.public_bytes(serialization.Encoding.PEM),'server.p12':pkcs12.serialize_key_and_certificates(name.encode(),key,cert,[ca],serialization.BestAvailableEncryption(password.encode())),'tls-password':password}
    credentials={name:secrets.token_urlsafe(24) for name in ('broker','admin','ingestion','processor','query')}
    tls=certificate('kafka',['secure-kafka','secure-kafka.context-graph.svc.cluster.local','*.secure-kafka-headless','*.secure-kafka-headless.context-graph.svc.cluster.local','localhost'])
    login='org.apache.kafka.common.security.plain.PlainLoginModule required username="broker" password="'+credentials['broker']+'" '+ ' '.join(f'user_{u}="{p}"' for u,p in credentials.items())+';'
    broker={**tls,'kafka.truststore.p12':trust,'jaas':login}
    outputs=[make_secret('kafka-broker-security',broker)]
    client_props={}
    for role in ('admin','ingestion','processor','query'):
        props='\n'.join(['security.protocol=SASL_SSL','sasl.mechanism=PLAIN',f'sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="{role}" password="{credentials[role]}";','ssl.truststore.type=PKCS12','ssl.truststore.location=/app/secrets/kafka.truststore.p12','ssl.truststore.password=changeit','ssl.endpoint.identification.algorithm=https',''])
        client_props[role]=props
        files={'kafka.properties':props,'kafka.truststore.p12':trust}
        if role in ('ingestion','query'):files.update(certificate(role,[role,f'{role}.context-graph.svc.cluster.local','localhost']))
        outputs.append(make_secret('security-'+role,files))
        write(f'{role}.properties',props)
    write('kafka.truststore.p12',trust)
    spice_token=secrets.token_urlsafe(48);pg_password=secrets.token_urlsafe(32)
    spice=certificate('spicedb',['spicedb','spicedb.context-graph.svc.cluster.local','localhost']);spice.update({'token':spice_token,'datastore-uri':f'postgres://spicedb:{pg_password}@spicedb-postgres:5432/spicedb?sslmode=disable'})
    outputs.extend([make_secret('spicedb-security',spice),make_secret('spicedb-client',{'token':spice_token}),make_secret('spicedb-postgres',{'password':pg_password})])
    # The issuer has an independent signing key, never mounted in APIs.
    signing=rsa.generate_private_key(public_exponent=65537,key_size=2048);pub=signing.public_key().public_numbers()
    b64=lambda n:base64.urlsafe_b64encode(n.to_bytes((n.bit_length()+7)//8,'big')).rstrip(b'=').decode()
    jwks={'keys':[{'kty':'RSA','use':'sig','alg':'RS256','kid':'local-v1','n':b64(pub.n),'e':b64(pub.e)}]}
    users={};user_credentials={}
    for user in ('admin','alice','bob','outsider','producer'):
        password=secrets.token_urlsafe(18);salt=secrets.token_bytes(16)
        users[user]={'subject':user,'salt':salt.hex(),'hash':hashlib.scrypt(password.encode(),salt=salt,n=16384,r=8,p=1).hex()};user_credentials[user]=password
    identity=certificate('identity',['identity','identity.context-graph.svc.cluster.local','localhost']);identity.update({'signing.key':signing.private_bytes(serialization.Encoding.PEM,serialization.PrivateFormat.PKCS8,serialization.NoEncryption()),'jwks.json':json.dumps(jwks),'users.json':json.dumps(users)})
    outputs.append(make_secret('identity-security',identity))
    outputs.append({'apiVersion':'v1','kind':'ConfigMap','metadata':{'name':'security-public-ca','namespace':NS},'data':{'ca.crt':ca_pem.decode()}})
    write('credentials.json',json.dumps(user_credentials,indent=2));write('spicedb-token',spice_token)
    write('secrets.json',json.dumps({'apiVersion':'v1','kind':'List','items':outputs}))

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--context');args=parser.parse_args();create()
    subprocess.run([__import__('sys').executable,'scripts/add-postgres-tls.py'],check=True)
    subprocess.run([__import__('sys').executable,'scripts/provision-check-proxy.py'],check=True)
    if args.context:subprocess.run(['kubectl','--context',args.context,'apply','-f',str(DIR/'secrets.json')],check=True)
    print('Security material prepared in .runtime/security (private files; no credentials printed).')
