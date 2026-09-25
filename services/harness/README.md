# Context Graph application harness

Build an application from JSON Schemas, Flink SQL views, and SQL-backed API queries. The compiler derives Iceberg output schemas and GraphQL result types. Operators provision the resulting release; builders do not receive platform credentials.

The harness is a set of independent tools. An AX task, a developer, or a CI pipeline can author and validate a bundle. Deployment remains a separate operator action. There is no required agent loop and no model gateway dependency.

## Included tools

| Command | Behavior |
| --- | --- |
| `cg init DIRECTORY` | Copy an editable example bundle |
| `cg build BUNDLE --output DIRECTORY` | Validate inputs, scope rules, Flink plans, and derived API types |
| `cg inspect DIRECTORY` | Show release metadata |
| `cg render DIRECTORY --node NODE` | Render Kubernetes resources without secrets |
| `cg deploy DIRECTORY --context CONTEXT --node NODE` | Recompile authored sources, provision scoped credentials and catalog tables, and deploy a candidate |
| `cg status DIRECTORY --context CONTEXT` | Inspect Kubernetes workloads and Flink REST state |
| `cg promote DIRECTORY --context CONTEXT` | Require running jobs, completed checkpoints, and healthy APIs before changing stable Service routes |

Use `scripts/cg` with Python dependencies from `requirements.txt`. Java compilation requires JDK 21 and Maven-built query and processor modules:

```sh
mvn -pl services/query,services/processor,services/ingestion -am install
python -m pip install -r services/harness/requirements.txt
python scripts/cg init my-app
python scripts/cg build my-app/bundle.yaml --output my-release
python scripts/cg inspect my-release
```

The output directory must be empty. Compiled sources and schemas are reviewable files. Builds prepare SQL against empty typed tables and validate Flink execution plans; they do not sample private application data.

## Bundle contract

See `examples/schema-data/bundle.yaml` and `examples/group-insights/bundle.yaml`. A bundle declares a workspace, JSON input schemas, one or more jobs, SQL views, and API fields. Each input gets its own ingestion route and Kafka topic. Each input is persisted as a schema-derived Iceberg table with a Kafka change topic. Each SQL view gets its own table and output topic. GraphQL mutations return a Kafka acceptance receipt; they do not claim immediate Iceberg visibility.

The example has separate activity and trajectory jobs. Activity produces individual events and a ten-second event-time aggregation. Trajectories use an ordinary JSON ingestion endpoint and table. There is no special trajectory database or first-class trajectory model.

## Security rails

- Builders cannot supply connector URLs, Kafka credentials, catalog credentials, deployment YAML, or arbitrary Java code through a bundle.
- Ingestion takes the permission resource key separately through `X-Resource-Key`; GraphQL mutations take `resourceKey`. JSON Schema defines the document without a required domain field.
- Scope columns `workspace_id`, `resource_id`, and `entity_id` must pass through unchanged. Aggregations must group by all three.
- SQL cannot join unrelated resources, erase scope through grouping sets, use arbitrary functions, read external files, or execute DDL.
- Flink validates input envelopes, payload schemas, and output scope/types. Invalid input envelopes go to the error topic.
- Each release has dedicated Kafka identities with topic ACLs and separate Polaris identities scoped to its catalog namespace. Provider credentials are not included in the bundle.
- Query and subscription access uses the existing SpiceDB checks. APIs reject identities outside the bundle workspace. Mutation forwarding retains the original user identity.
- Deployment recompiles the authored source instead of trusting edited generated manifests.

Applications that combine differently restricted sources need an explicit derived-data access policy. V1 rejects such joins rather than silently widening access.

## Runtime and deployment

The existing local platform, security issuer/CA, Kafka, SpiceDB, object storage, Polaris, and `flink-runtime` service account must exist before deployment. Build runtime images from the repository Dockerfiles with these tags:

```sh
docker build -f services/ingestion/Dockerfile -t context-graph/ingestion:schema-v1 .
docker build -f services/processor/Dockerfile -t context-graph/processor:schema-v1 .
docker build -f services/query/Dockerfile -t context-graph/query:schema-v1 .
python scripts/cg deploy my-release --context YOUR_CONTEXT --node YOUR_STORAGE_NODE
python scripts/cg status my-release --context YOUR_CONTEXT
python scripts/cg promote my-release --context YOUR_CONTEXT
```

Flink jobs run as ordinary JobManager and TaskManager Deployments with Kubernetes HA metadata. No Flink operator is needed. Checkpoints use incremental RocksDB state, one concurrent checkpoint, and object-storage recovery paths.

Promotion changes API Service selectors. It does not transfer input events or Flink state between different release namespaces. Each release has its own topics and tables. Repointing to an old release restores its routes, not data written only to a newer release. Production upgrades that require continuous history need an explicit replay/state migration plan; this prototype does not claim automatic zero-downtime state migration.

## Building with AX

`builder/ax.yaml` is an AX manifest for invoking the same compiler tools. Build the pinned upstream runner with `scripts/build-ax-tools.sh`, then build `builder/Dockerfile`. AX receives workspace files and compiler tooling, not deployment credentials. The same client library can query and ingest using an explicitly delegated user identity.

The [AX execution guide](../../docs/ax.md) connects the builder workflow to the running assistant example. Every assistant question uses AX: the coordinator delegates retrieval through run-specific capabilities, checks the caller's permissions, and streams a cited answer. The same execution infrastructure can run the builder task with its own workspace and compiler tools.

## Current limits

V1 bundle inputs are JSON. The platform's existing media ingestion remains available, but the bundle compiler does not yet generate media endpoints. Flink outputs are append-only and support structs, arrays, decimals, and native VARIANT. `_json_remainder` retains unmapped keys at each structured object level. See [schema mapping](../../docs/json-schema-storage.md). Jobs consume declared bundle inputs, not arbitrary existing Kafka topics. There is no generated durable command-status store, general rollback/state migration command, background investigation inbox, or autonomous PR author yet.


The checked-in group-insights bundle is a candidate example. Its deployment requires validation against the replacement schema-defined runtime. The local candidate is left scaled to zero and unpromoted while the AX trial is running. Do not treat that candidate as a production-ready release.
