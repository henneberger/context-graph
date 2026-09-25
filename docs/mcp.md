# Authenticated MCP access

The MCP service exposes registered datasets and SQL-backed GraphQL queries to MCP clients. Requests execute through the serving API under the signed-in user's identity. SpiceDB checks workspace and resource permissions before SQL processing and before results are returned.

## Tools

| Tool | Purpose |
| --- | --- |
| `list_datasets` | List registered dataset columns; requires workspace schema visibility. |
| `describe_api` | Discover GraphQL query fields, arguments, and result types. |
| `query_data` | Execute a single GraphQL query with optional variables. |

The service rejects mutations, subscriptions, and documents containing multiple operations. Query parameters cannot select another identity, workspace, backend URL, or catalog credential. Dataset contents remain untrusted input to the consuming assistant.

## Authorization

The service uses Streamable HTTP with OAuth protected-resource discovery. The development issuer supports dynamic client registration, authorization code flow with S256 PKCE, browser consent, refresh rotation, and revocation. Clients request the `context:read` scope for the MCP resource URL. Consent binds a grant to one workspace; SpiceDB remains authoritative for access within that workspace.

MCP access tokens are opaque, expire after five minutes, and are checked against shared PostgreSQL state on every request. Authorization codes expire after one minute and can be used once. Grants last at most 24 hours. Revoking an access or refresh token revokes its grant, including other tokens issued under that grant. Refresh tokens rotate on use.

For a data request, MCP exchanges its token at the trusted issuer for a user-bound API token lasting at most 60 seconds. That token carries the consented workspace and read-only scope. The serving API enforces both, including denial of ingestion and workspace management. OAuth revocation prevents subsequent MCP exchanges; an already-issued API token remains valid until expiry, subject to current SpiceDB permissions.

OAuth state is shared across issuer replicas. Only token hashes are used as database lookup keys. The MCP service holds no administrator credentials, direct lakehouse access, or object-storage credentials.

## Local deployment

The standard deployment provisions MCP certificates, a dedicated OAuth database and role, and Kubernetes network policies. The MCP Deployment has two replicas, readiness probes, and Prometheus metrics on port 9404. The issuer's readiness probe includes the OAuth listener and database.

For an existing deployment, build the service images and run the deployment scripts described in the [README](../README.md). The development profile uses these loopback forwards in separate terminals:

```sh
kubectl --context YOUR_CONTEXT -n context-graph port-forward service/mcp 18090:8443 --address 127.0.0.1
kubectl --context YOUR_CONTEXT -n context-graph port-forward service/identity 18445:8444 --address 127.0.0.1
```

TLS remains enabled on both connections. The development CA is `.runtime/security/ca.crt`. The browser used for sign-in must trust that CA; the Desktop bridge receives its path explicitly.

## Claude Desktop

Create a Python environment and install the client dependencies:

```sh
python3 -m venv .runtime/mcp-venv
.runtime/mcp-venv/bin/pip install -r services/mcp/requirements.txt
```

Add the following entry to the `mcpServers` object in the Claude Desktop configuration, replacing `/absolute/path/context-graph` with the repository location:

```json
{
  "context-graph": {
    "command": "/absolute/path/context-graph/.runtime/mcp-venv/bin/python",
    "args": ["/absolute/path/context-graph/services/mcp/desktop.py"],
    "env": {
      "MCP_URL": "https://localhost:18090/mcp",
      "CA_FILE": "/absolute/path/context-graph/.runtime/security/ca.crt"
    }
  }
}
```

The bridge opens a browser for sign-in and consent, then forwards MCP tool calls over authenticated HTTPS. The callback listener binds only to `127.0.0.1:18191`. Change `MCP_CALLBACK_PORT` when that port is occupied. Tokens and client registration are stored under `~/.config/context-graph/mcp/` in an endpoint-specific directory with mode 0700 and files with mode 0600. `MCP_TOKEN_DIR` overrides that location. Deleting this directory requires registration and sign-in again; use OAuth revocation to invalidate an existing grant.

## Remote connectors

A remote connector requires a publicly reachable HTTPS deployment with publicly trusted certificates for both MCP and the authorization server. Configure:

| Service | Setting | Value |
| --- | --- | --- |
| MCP | `MCP_PUBLIC_URL` | Externally reachable MCP URL, including `/mcp` |
| MCP | `MCP_AUTH_ISSUER` | External authorization issuer URL |
| Issuer | `OAUTH_PUBLIC_URL` | The same external issuer URL |
| Issuer | `MCP_PUBLIC_URL` | The same MCP resource URL |
| MCP | `MCP_INTROSPECTION_URL` | Trusted internal token-verification endpoint |
| MCP | `MCP_EXCHANGE_URL` | Trusted internal delegated-token endpoint |
| MCP | `QUERY_URL` | Internal serving API GraphQL endpoint |

Configure ingress routes, certificates, and ingress-controller access in the NetworkPolicies for MCP port 8443 and issuer port 8444. Preserve authorization headers and the external resource URLs. The local profile binds resource metadata and OAuth grants to its loopback URLs; change these settings before registering remote clients.

The bundled issuer is for development. A production OAuth provider must supply compatible introspection and user-token exchange, or an adapter implementing those contracts. Setting an issuer URL alone does not integrate a different provider. `/exchange` is the bundled issuer's internal exchange contract.

The repository does not provision a public endpoint. Configure the deployed MCP URL in the remote client's connector settings after ingress and identity integration are available.

## Verification

With the local API and the two forwards running:

```sh
.runtime/mcp-venv/bin/python -m unittest discover -s services/mcp/tests -v
.runtime/mcp-venv/bin/python scripts/mcp-smoke.py
```

The deployment smoke test uses local fixture credentials without printing tokens. It verifies OAuth discovery, PKCE, CSRF protection, code replay denial, permission-filtered reads, rejected writes, workspace binding, refresh rotation, and revocation. It requires the `demo` workspace fixtures and at least one record readable by `alice` under resource `alpha`.

Metrics include `context_mcp_tool_calls_total` and `context_mcp_tool_duration_seconds`, labeled by tool and outcome. Credentials, query variables, and returned records are excluded from metric labels.
