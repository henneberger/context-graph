#!/usr/bin/env python3
"""Provision local MCP TLS and isolated OAuth state, without exposing credentials."""

import argparse, base64, datetime as dt, json, secrets, subprocess
from pathlib import Path
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID, ExtendedKeyUsageOID

p = argparse.ArgumentParser()
p.add_argument("--context", required=True)
a = p.parse_args()
base = ["kubectl", "--context", a.context, "-n", "context-graph"]
root = Path(".runtime/security")
state = root / "mcp-provision.json"
saved = json.loads(state.read_text()) if state.exists() else {}
renew = not saved
if saved:
    cert = x509.load_pem_x509_certificate(saved["cert"].encode())
    try:
        cert.extensions.get_extension_for_class(x509.AuthorityKeyIdentifier)
    except x509.ExtensionNotFound:
        renew = True
    renew = renew or cert.not_valid_after_utc < dt.datetime.now(
        dt.timezone.utc
    ) + dt.timedelta(days=7)
if renew:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    ca = x509.load_pem_x509_certificate((root / "ca.crt").read_bytes())
    cakey = serialization.load_pem_private_key((root / "ca.key").read_bytes(), None)
    now = dt.datetime.now(dt.timezone.utc)
    cert = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "mcp")]))
        .issuer_name(ca.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - dt.timedelta(minutes=5))
        .not_valid_after(now + dt.timedelta(days=90))
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), True)
        .add_extension(
            x509.SubjectAlternativeName(
                [
                    x509.DNSName(n)
                    for n in ("mcp", "mcp.context-graph.svc.cluster.local", "localhost")
                ]
            ),
            False,
        )
        .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), False)
        .add_extension(
            x509.SubjectKeyIdentifier.from_public_key(key.public_key()), False
        )
        .add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_public_key(cakey.public_key()),
            False,
        )
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=True,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=False,
                crl_sign=False,
                encipher_only=None,
                decipher_only=None,
            ),
            True,
        )
        .sign(cakey, hashes.SHA256())
    )
    saved = {
        "password": saved.get("password") or secrets.token_urlsafe(32),
        "cert": cert.public_bytes(serialization.Encoding.PEM).decode(),
        "key": key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        ).decode(),
    }
    state.write_text(json.dumps(saved))
    state.chmod(0o600)
pw = saved["password"]
sql = (
    "SELECT 'CREATE ROLE context_oauth LOGIN PASSWORD ''"
    + pw
    + "''' WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname='context_oauth')\\gexec\nSELECT 'CREATE DATABASE context_oauth OWNER context_oauth' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname='context_oauth')\\gexec\n"
)
subprocess.run(
    base
    + [
        "exec",
        "-i",
        "spicedb-postgres-0",
        "--",
        "psql",
        "-U",
        "spicedb",
        "-d",
        "postgres",
        "-v",
        "ON_ERROR_STOP=1",
    ],
    input=sql,
    text=True,
    check=True,
    stdout=subprocess.DEVNULL,
)


def secret(name, data):
    return {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": name, "namespace": "context-graph"},
        "data": {k: base64.b64encode(v.encode()).decode() for k, v in data.items()},
    }


items = [
    secret(
        "mcp-security",
        {
            "server.crt": saved["cert"],
            "server.key": saved["key"],
            "ca.crt": (root / "ca.crt").read_text(),
        },
    ),
    secret(
        "identity-oauth",
        {
            "database-uri": f"postgresql://context_oauth:{pw}@spicedb-postgres:5432/context_oauth?sslmode=verify-full&sslrootcert=/run/oauth-ca/ca.crt"
        },
    ),
]
subprocess.run(
    base + ["apply", "-f", "-"],
    input=json.dumps({"apiVersion": "v1", "kind": "List", "items": items}),
    text=True,
    check=True,
)
