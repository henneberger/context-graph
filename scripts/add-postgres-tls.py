"""Upgrade generated local security bundle to verified PostgreSQL TLS (idempotent)."""
import base64,datetime,json
from pathlib import Path
from cryptography import x509
from cryptography.hazmat.primitives import hashes,serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
p=Path('.runtime/security/secrets.json');bundle=json.loads(p.read_text());entries={x['metadata']['name']:x for x in bundle['items']}
if 'server.crt' not in entries['spicedb-postgres']['data']:
    root=p.parent;ca=x509.load_pem_x509_certificate((root/'ca.crt').read_bytes());key=serialization.load_pem_private_key((root/'ca.key').read_bytes(),None)
    server=rsa.generate_private_key(public_exponent=65537,key_size=2048);now=datetime.datetime.now(datetime.timezone.utc)
    cert=x509.CertificateBuilder().subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME,'spicedb-postgres')])).issuer_name(ca.subject).public_key(server.public_key()).serial_number(x509.random_serial_number()).not_valid_before(now-datetime.timedelta(minutes=5)).not_valid_after(now+datetime.timedelta(days=90)).add_extension(x509.SubjectAlternativeName([x509.DNSName('spicedb-postgres'),x509.DNSName('spicedb-postgres.context-graph.svc.cluster.local')]),False).add_extension(x509.BasicConstraints(ca=False,path_length=None),True).add_extension(x509.SubjectKeyIdentifier.from_public_key(server.public_key()),False).add_extension(x509.AuthorityKeyIdentifier.from_issuer_public_key(key.public_key()),False).sign(key,hashes.SHA256())
    enc=lambda b:base64.b64encode(b).decode()
    entries['spicedb-postgres']['data'].update({'server.crt':enc(cert.public_bytes(serialization.Encoding.PEM)),'server.key':enc(server.private_bytes(serialization.Encoding.PEM,serialization.PrivateFormat.PKCS8,serialization.NoEncryption()))})
    spice=entries['spicedb-security']['data'];spice['ca.crt']=enc((root/'ca.crt').read_bytes());spice['datastore-uri']=enc(base64.b64decode(spice['datastore-uri']).replace(b'sslmode=disable',b'sslmode=verify-full&sslrootcert=/run/spicedb/ca.crt'))
    p.write_text(json.dumps(bundle));p.chmod(0o600)
