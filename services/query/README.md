# Authorized query API

Java 21 / Vert.x 5.2.0. Build shared security first: `mvn -f services/security/pom.xml install`, then `mvn -f services/query/pom.xml package`. The Dockerfile includes that build order. Mandatory configuration is documented in docs/security-architecture.md. There is no anonymous or auth-disabled query mode.

HTTP POST `/graphql` requires `Authorization: Bearer <verified OIDC JWT>` and `X-Workspace-Id`. Workspace selection is untrusted until SpiceDB confirms membership. HTTP development transport requires explicit `SECURITY_ALLOW_HTTP=true`; otherwise TLS_KEYSTORE_PATH and TLS_KEYSTORE_PASSWORD are mandatory. Kafka always requires the SASL_SSL secret properties file identified by KAFKA_CLIENT_CONFIG, with broker hostname verification enabled.

Websocket `/graphql` negotiates **graphql-transport-ws**. Send `connection_init` with payload `{ "authorization": "Bearer ...", "workspaceId": "..." }`. The server acknowledges only after JWT verification and a fully consistent workspace permission check. Each connection has an immutable identity and an expiry timer; reconnect to refresh credentials. No caller-provided subject/group headers or cookies authenticate this endpoint.

Every Kafka emission checks workspace, canonical resource provenance, token expiry, current workspace access, and SpiceDB entity:view. Edges require both endpoint permissions. Broad subscriptions omit explicitly forbidden resources. A requested entity's revocation terminates its subscription. Backend failures, membership revocation, and expiry fail closed. No positive authorization cache is used. Checks cannot retract bytes already delivered.

Only `registeredTables` with required security columns are exposed. Legacy/unlabeled tables do not appear in schema discovery or generated GraphQL fields. Schema discovery requires view_schema and never returns physical metadata paths. Configured and generated table queries use the same authorization path:

1. Scan candidate resource IDs only in the selected workspace; verify provenance and current grants.
2. Construct an allowlist, including both endpoints for edges; materialize authorized source rows before evaluating any configured aggregate/limit/join.
3. Remove the raw view and allowlist, disable DuckDB external access, and lock configuration. Execute a restricted single SELECT/CTE against source.
4. Recheck workspace and contributor permissions before returning results.

`maxSourceRows` and `maxSourceResources` fail the query on overflow instead of returning a misleading partial aggregate. Query workers/queues are bounded, memory is limited per DuckDB connection, SQL has a timeout, and authorization loops enforce the query deadline. These limits may reject broad historical scans; narrow the registered source/query design or increase capacity deliberately. No result cache is shared across principals.

The service loads both iceberg and cache_httpfs at startup. The container build preinstalls matching platform/version extensions under /opt/duckdb-extensions, so unprivileged startup does not write to a home directory or download extensions. Native runs use an explicit writable temporary extension directory unless DUCKDB_EXTENSION_DIRECTORY is set. cache_httpfs benefits remote HTTP/object reads, not native local scans. Hadoop metadata resolution follows committed version-hint files, with no highest-filename guessing.

`mvn test` covers actual DuckDB parameter binding, authorization before aggregates, two disjoint users and another workspace, both edge endpoints, post-computation revocation, overflow, SQL escape attempts, subscription revocation/expiry/outage, and schema rules. `TEST_DUCKDB_EXTENSIONS=true mvn test` additionally loads both native extensions. Real deployment smoke must use the provisioned OIDC issuer, SpiceDB, and role-restricted Kafka credentials; the earlier unauthenticated query-smoke workflow is obsolete.
