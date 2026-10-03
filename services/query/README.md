# Authorized query API

Java 21 / Vert.x 5.2.0. Build shared security first: `mvn -f services/security/pom.xml install`, then `mvn -f services/query/pom.xml package`. The Dockerfile includes that build order. Mandatory configuration is documented in docs/security-architecture.md. There is no anonymous or auth-disabled query mode.

HTTP POST `/graphql` requires `Authorization: Bearer <verified OIDC JWT>` and `X-Workspace-Id`. Workspace selection is untrusted until SpiceDB confirms membership. HTTP development transport requires explicit `SECURITY_ALLOW_HTTP=true`; otherwise TLS_KEYSTORE_PATH and TLS_KEYSTORE_PASSWORD are mandatory. Kafka always requires the SASL_SSL secret properties file identified by KAFKA_CLIENT_CONFIG, with broker hostname verification enabled.

Websocket `/graphql` negotiates **graphql-transport-ws**. Send `connection_init` with payload `{ "authorization": "Bearer ...", "workspaceId": "..." }`. The server acknowledges only after JWT verification and a fully consistent workspace permission check. Each connection has an immutable identity and an expiry timer; reconnect to refresh credentials. No caller-provided subject/group headers or cookies authenticate this endpoint.

Every Kafka emission checks workspace, canonical resource provenance, token expiry, current workspace access, and SpiceDB entity:view. Edges require both endpoint permissions. Broad subscriptions omit explicitly forbidden resources. A requested entity's revocation terminates its subscription. Backend failures, membership revocation, and expiry fail closed. No positive authorization cache is used. Checks cannot retract bytes already delivered.

Only `registeredTables` with required security columns are exposed. Legacy/unlabeled tables do not appear in schema discovery or generated GraphQL fields. Schema discovery requires view_schema and never returns physical metadata paths. Configured and generated table queries use the same authorization path:

1. Scan candidate resource IDs only in the selected workspace; verify provenance and current grants.
2. Construct an allowlist, including additional resources only where explicitly registered by policy; materialize authorized source rows before evaluating any configured aggregate/limit/join.
3. Remove the raw view and allowlist, disable DuckDB external access, and lock configuration. Execute a restricted single SELECT/CTE against source.
4. Recheck workspace and contributor permissions before returning results.

`maxSourceRows` and `maxSourceResources` fail the query on overflow instead of returning a misleading partial aggregate. Query workers/queues are bounded, memory is limited per DuckDB connection, SQL has a timeout, and authorization loops enforce the query deadline. These limits may reject broad historical scans; narrow the registered source/query design or increase capacity deliberately. No result cache is shared across principals.

The service loads both iceberg and cache_httpfs at startup. The container build preinstalls matching platform/version extensions under /opt/duckdb-extensions, so unprivileged startup does not write to a home directory or download extensions. Native runs use an explicit writable temporary extension directory unless DUCKDB_EXTENSION_DIRECTORY is set. cache_httpfs benefits remote HTTP/object reads, not native local scans. Hadoop metadata resolution follows committed version-hint files, with no highest-filename guessing.

`mvn test` covers actual DuckDB parameter binding, authorization before aggregates, two disjoint users and another workspace, both edge endpoints, post-computation revocation, overflow, SQL escape attempts, subscription revocation/expiry/outage, and schema rules. `TEST_DUCKDB_EXTENSIONS=true mvn test` additionally loads both native extensions. Real deployment smoke must use the provisioned OIDC issuer, SpiceDB, and role-restricted Kafka credentials; the earlier unauthenticated query-smoke workflow is obsolete.

## OrchidDB graph queries

The query service uses `com.orchiddb:orchiddb-java:0.1.0`; graph queries also require its matching native compiler. Set a query's `engine: orchiddb`, `graph`, `language` (`cypher`, `gremlin`, or `sparql`), and `query` instead of `sql`. Existing GraphQL signatures and `sources` registration still apply. Top-level `graphs` hold reusable `nodes`, `edges`, and `ontology` mappings. [The complete example](../../examples/ontology/queries.yaml) exposes Cypher friends, SPARQL vocabulary friends, and a person count through the same graph. Select that file with the `queries` setting in your query-service configuration, after publishing its registered tables.

The example expects these columns, in addition to the trusted security labels written by the ingestion/processing path:

| Table | Data columns | Security columns |
| --- | --- | --- |
| `context_secure.people` | `person_id BIGINT`, `full_name STRING`, `person_iri STRING`; optional unmapped fields | `workspace_id`, `resource_id`, `entity_id` |
| `context_secure.knows` | `edge_id BIGINT`, `from_id BIGINT`, `to_id BIGINT` | `workspace_id`, `resource_id`, `source_entity_id`, `target_resource_id`, `target_entity_id` |

Graph IDs and edge endpoints must be signed integers; IDs must be stable, unique, and non-null within each label/type. Keep the string security entity IDs and canonical resource IDs separate from graph IDs. `from_id` and `to_id` reference `person_id`; edge resource labels must identify those same endpoint entities. The ingestion/application contract must maintain that correspondence. `person_iri` supplies RDF subject identity through the mapped `iri` property. The Java ontology relationship mapping follows source-to-target direction; represent inverse vocabulary relationships with an explicit reversed edge mapping. This layer maps vocabulary, without automatic OWL reasoning.

Example GraphQL requests are `{ friends(name: "Ada") { name } }`, `{ vocabularyFriends { name } }`, and `{ personCount { total } }`. Cypher GraphQL arguments are passed as typed compiler parameters, never interpolated into text. This release does not support bindings for Gremlin or SPARQL, or all graph-language constructs; unsupported queries fail explicitly. Use literal query limits (as in the example); parameterized Cypher `LIMIT` does not lower to SQL in this pinned release.

Each source passes the existing workspace, provenance, and SpiceDB checks before entering a fresh DuckDB connection. Edges with a `targetEntityColumn` require both endpoint grants. OrchidDB sees projections of configured columns from those authorized relations only, so unmapped VARIANT fields do not break metadata discovery. Traversals and aggregates run after filtering, with no catalog credentials or external access; contributor permissions are checked again before returning results. Source/resource limits still apply, graph output overflow fails instead of truncating, and execution observes the remaining query deadline. No graph plan, result, or statistics cache is shared across callers. Compilation itself is synchronous and checked against the deadline on return; native compiler work is not preempted by the SQL timeout.

## Growing the context graph

Extend a named graph by adding node labels, edge types, and mapped properties to its configuration; queries can reuse that graph or select a separate domain mapping. Add each source to `registeredTables` and to the queries that use it. New sources keep their own authorization policies, and edge sources must register both endpoint labels. The integration test extends the people graph with projects and assignments and verifies that existing friend queries still work alongside the new traversal.

Use stable entity IDs across source updates, preserve source references and event times as queryable properties, and carry provenance through derived relationships. Keep versioned ontology IRIs and subject identities independent of physical column names. Incompatible vocabulary changes should introduce a new named graph while existing consumers migrate. Flink jobs or application tasks publish new facts through the existing ingestion path; OrchidDB queries those facts through the same permissions. Graph configuration reloads with the query service's schema refresh.

Growth remains bounded per request: the current serving path supports up to eight registered source aliases and enforces row, resource, memory, output, and time limits. As the overall graph grows, publish focused domain views and derived relationships with processing jobs rather than increasing every request's working set. This integration does not add automatic entity resolution, ontology inference, or persistent graph indexes.

## Native compiler setup

On macOS ARM64, Maven includes the matching published native classifier automatically. With Java 21 and Maven, run from the repository root:

```sh
mvn -pl services/query -am verify
java -cp services/query/target/query-0.1.0-SNAPSHOT.jar io.contextgraph.query.OrchidNativeCheck
```

This repository does not build native compilers. On other platforms, provide a compatible OrchidDB 0.1.0 JNI library separately and set `-Dorchiddb.native.path=/absolute/path/to/library` for tests or through `JAVA_TOOL_OPTIONS` when starting the service. The Java 0.1.0 package expects compiler revision `18ad70a9ad461e126a739b5c99f10c13622f33e6`; use a matching build from OrchidDB. The standard Linux container retains SQL serving without a compiler; configuring graph queries requires that separately supplied library and fails closed if it is unavailable. No Linux native build or release automation is included.
