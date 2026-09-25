#!/usr/bin/env python3
"""Verify a deployed MCP OAuth flow and permission boundaries using local fixtures."""

import argparse
import base64
import hashlib
import json
import secrets
import ssl
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import httpx

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--mcp-url", default="https://localhost:18090/mcp")
parser.add_argument("--issuer", default="https://localhost:18445")
parser.add_argument("--api", default="http://localhost:18088")
parser.add_argument("--ca", default=".runtime/security/ca.crt")
parser.add_argument("--credentials", default=".runtime/security/credentials.json")
a = parser.parse_args()
credentials = json.loads(Path(a.credentials).read_text())
client = httpx.Client(
    verify=ssl.create_default_context(cafile=a.ca), timeout=60, follow_redirects=False
)


def check(response, status):
    assert response.status_code == status, (
        f"Unexpected HTTP status: {response.status_code}, expected {status}"
    )
    return response


metadata = check(
    client.get(a.issuer + "/.well-known/oauth-authorization-server"), 200
).json()
assert metadata["issuer"] == a.issuer
unauthorized = check(client.post(a.mcp_url, json={}), 401)
assert "resource_metadata=" in unauthorized.headers["www-authenticate"]
resource_metadata = (
    unauthorized.headers["www-authenticate"]
    .split('resource_metadata="')[1]
    .split('"')[0]
)
assert check(client.get(resource_metadata), 200).json()["resource"] == a.mcp_url
registration = check(
    client.post(
        metadata["registration_endpoint"],
        json={
            "client_name": "Platform verification",
            "redirect_uris": ["http://127.0.0.1:18192/callback"],
            "token_endpoint_auth_method": "none",
            "grant_types": ["authorization_code", "refresh_token"],
            "response_types": ["code"],
            "scope": "context:read",
        },
    ),
    201,
).json()
client_id = registration["client_id"]
redirect = registration["redirect_uris"][0]


def authorize(user, workspace="demo"):
    verifier = secrets.token_urlsafe(48)
    challenge = (
        base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest())
        .rstrip(b"=")
        .decode()
    )
    state = secrets.token_urlsafe(24)
    params = dict(
        client_id=client_id,
        response_type="code",
        redirect_uri=redirect,
        scope="context:read",
        resource=a.mcp_url,
        state=state,
        code_challenge=challenge,
        code_challenge_method="S256",
    )
    denied = client.get(
        metadata["authorization_endpoint"],
        params={**params, "resource": "https://other.invalid/mcp"},
    )
    assert denied.status_code in (302, 400)
    if denied.status_code == 302:
        assert "error=" in denied.headers["location"]
    location = check(
        client.get(metadata["authorization_endpoint"], params=params), 302
    ).headers["location"]
    check(client.get(location), 200)
    csrf = client.cookies.get("cg_oauth_csrf")
    form = dict(
        username=user,
        password=credentials[user],
        workspace=workspace,
        csrf=csrf,
        approve="yes",
    )
    check(client.post(location, data={**form, "csrf": "invalid"}), 400)
    response = check(client.post(location, data=form), 303)
    values = parse_qs(urlsplit(response.headers["location"]).query)
    assert values["state"] == [state]
    request = dict(
        grant_type="authorization_code",
        code=values["code"][0],
        client_id=client_id,
        redirect_uri=redirect,
        code_verifier=verifier,
        resource=a.mcp_url,
    )
    check(
        client.post(
            metadata["token_endpoint"], data={**request, "code_verifier": "wrong" * 16}
        ),
        400,
    )
    token = check(client.post(metadata["token_endpoint"], data=request), 200).json()
    check(client.post(metadata["token_endpoint"], data=request), 400)
    return token


def rpc(token, method, params=None):
    response = check(
        client.post(
            a.mcp_url,
            headers={
                "Authorization": "Bearer " + token["access_token"],
                "Accept": "application/json, text/event-stream",
                "MCP-Protocol-Version": "2025-03-26",
            },
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": method,
                **({"params": params} if params is not None else {}),
            },
        ),
        200,
    ).json()
    assert "error" not in response, "MCP protocol error"
    return response["result"]


def query(token, document):
    result = rpc(
        token, "tools/call", {"name": "query_data", "arguments": {"query": document}}
    )
    assert not result.get("isError"), "MCP query failed"
    return result.get("structuredContent") or json.loads(result["content"][0]["text"])


alice = authorize("alice")
info = rpc(
    alice,
    "initialize",
    {
        "protocolVersion": "2025-03-26",
        "capabilities": {},
        "clientInfo": {"name": "verification", "version": "1"},
    },
)
assert info["serverInfo"]["name"] == "Context Graph"
tools = rpc(alice, "tools/list")["tools"]
assert {t["name"] for t in tools} == {"list_datasets", "describe_api", "query_data"}
assert all(t["annotations"]["readOnlyHint"] for t in tools)
assert not rpc(alice, "tools/call", {"name": "describe_api", "arguments": {}}).get(
    "isError"
)
assert not rpc(alice, "tools/call", {"name": "list_datasets", "arguments": {}}).get(
    "isError"
)
rows = query(alice, '{records(entityId:"alpha",limit:10){event_id entity_id}}')[
    "records"
]
assert rows and all(r["entity_id"] == "alpha" for r in rows)
bob = authorize("bob")
assert query(bob, '{records(entityId:"alpha",limit:10){event_id}}')["records"] == []
for document in (
    "mutation { write }",
    "subscription { changes }",
    "query A {schemas{name}} mutation B {write}",
):
    result = rpc(
        alice, "tools/call", {"name": "query_data", "arguments": {"query": document}}
    )
    assert result.get("isError"), "Non-query operation accepted"
print(
    "PASS OAuth discovery, PKCE, CSRF, single-use codes, MCP tools and resource isolation",
    flush=True,
)

producer = authorize("producer")
delegated = check(
    client.post(
        a.issuer + "/exchange",
        headers={"Authorization": "Bearer " + producer["access_token"]},
    ),
    200,
).json()["access_token"]
headers = {
    "Authorization": "Bearer " + delegated,
    "X-Workspace-Id": "demo",
    "X-Resource-Key": "alpha",
}
check(
    client.post(
        a.api + "/ingest/events",
        headers=headers,
        json={"metric": "mcp-write-denied", "value": 1},
    ),
    403,
)
check(
    client.post(
        a.api + "/graphql",
        headers={**headers, "X-Workspace-Id": "knowledge"},
        json={"query": "{schemas{name}}"},
    ),
    403,
)
print("PASS exchanged token cannot write or cross workspaces", flush=True)

refreshed = check(
    client.post(
        metadata["token_endpoint"],
        data={
            "grant_type": "refresh_token",
            "client_id": client_id,
            "refresh_token": alice["refresh_token"],
            "resource": a.mcp_url,
        },
    ),
    200,
).json()
check(
    client.post(
        metadata["token_endpoint"],
        data={
            "grant_type": "refresh_token",
            "client_id": client_id,
            "refresh_token": alice["refresh_token"],
            "resource": a.mcp_url,
        },
    ),
    400,
)
assert query(refreshed, '{records(entityId:"alpha",limit:1){event_id}}')["records"]
check(
    client.post(
        metadata["revocation_endpoint"],
        data={
            "token": refreshed["refresh_token"],
            "token_type_hint": "refresh_token",
            "client_id": client_id,
        },
    ),
    200,
)
check(
    client.post(
        a.mcp_url,
        headers={"Authorization": "Bearer " + refreshed["access_token"]},
        json={},
    ),
    401,
)
for token in (bob, producer):
    check(
        client.post(
            metadata["revocation_endpoint"],
            data={
                "token": token["refresh_token"],
                "token_type_hint": "refresh_token",
                "client_id": client_id,
            },
        ),
        200,
    )
print("PASS refresh rotation, replay denial and immediate grant revocation", flush=True)
