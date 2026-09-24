#!/usr/bin/env python3
"""Provision separate permission-check credentials and TLS certificate; never print secrets."""
import argparse
import base64
import datetime
import json
import os
from pathlib import Path
import secrets
import subprocess
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID, ExtendedKeyUsageOID

parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--context',help='Apply to this Kubernetes context; omit to prepare secret files only')
args=parser.parse_args()
root = Path('.runtime/security')
ca = x509.load_pem_x509_certificate((root / 'ca.crt').read_bytes())
ca_key = serialization.load_pem_private_key((root / 'ca.key').read_bytes(), password=None)
key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
now = datetime.datetime.now(datetime.timezone.utc)
cert = (x509.CertificateBuilder().subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, 'spicedb-checks')]))
        .issuer_name(ca.subject).public_key(key.public_key()).serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=5)).not_valid_after(now + datetime.timedelta(days=90))
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(key.public_key()), critical=False)
        .add_extension(x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()), critical=False)
        .add_extension(x509.SubjectAlternativeName([x509.DNSName(name) for name in ('spicedb-checks', 'spicedb-checks.context-graph.svc.cluster.local', 'localhost')]), critical=False)
        .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), critical=False).sign(ca_key, hashes.SHA256()))
client_file = root / 'spicedb-client-token'
client = client_file.read_text().strip() if client_file.exists() else secrets.token_urlsafe(48)
client_file.write_text(client); os.chmod(client_file, 0o600)
data = {'server.crt': cert.public_bytes(serialization.Encoding.PEM), 'server.key': key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()),
        'client-token': client.encode(), 'backend-token': (root / 'spicedb-token').read_bytes(), 'ca.crt': (root / 'ca.crt').read_bytes()}
items = [{'apiVersion': 'v1', 'kind': 'Secret', 'metadata': {'name': 'check-proxy-security', 'namespace': 'context-graph'}, 'type': 'Opaque', 'data': {name: base64.b64encode(value).decode() for name, value in data.items()}},
         {'apiVersion': 'v1', 'kind': 'Secret', 'metadata': {'name': 'spicedb-client', 'namespace': 'context-graph'}, 'type': 'Opaque', 'data': {'token': base64.b64encode(client.encode()).decode()}}]
manifest = root / 'check-proxy-secrets.json'
manifest.write_text(json.dumps({'apiVersion': 'v1', 'kind': 'List', 'items': items})); os.chmod(manifest, 0o600)
bundle_path=root/'secrets.json'
if bundle_path.exists():
    bundle=json.loads(bundle_path.read_text()); replaced={item['metadata']['name'] for item in items}
    bundle['items']=[item for item in bundle['items'] if item.get('metadata',{}).get('name') not in replaced]+items
    bundle_path.write_text(json.dumps(bundle)); os.chmod(bundle_path,0o600)
if args.context:
    subprocess.run(['kubectl', '--context', args.context, 'apply', '-f', str(manifest)], check=True, stdout=subprocess.DEVNULL)
print('Provisioned separate check-only credential and TLS proxy secrets.')
