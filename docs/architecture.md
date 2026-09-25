# Platform architecture

![Platform data paths](diagrams/context-graph-overview.svg)

JSON Schemas define datasets. Application SQL defines transformations and views. The platform supplies transport, execution, permissions, catalog/storage access, and APIs. It does not create a domain ontology or require node and edge tables.

## Data path

Vert.x ingestion validates the endpoint's JSON Schema, verifies write access, stamps a security envelope, and publishes to the endpoint's Kafka topic. Images and video store their bytes privately; their extracted metadata follows the same event path. Invalid processing records have an error queue.

Flink's standard Kafka source feeds a JSON Schema adapter. Declared fields become typed relational columns, including nested structs and arrays. Unconstrained values use VARIANT, and undeclared keys live in `_json_remainder VARIANT` at the appropriate object level. Source datasets persist automatically; optional SQL views can write additional Iceberg tables and Kafka change streams. See the [precise mapping contract](json-schema-storage.md).

Iceberg format-version 3 tables live in RustFS object storage. Polaris manages the REST catalog and vends storage credentials to service identities. Warehouse files, media, and recovery state use separate private buckets. PostgreSQL supports authorization, catalog, and connector services.

The serving API discovers registered Iceberg schemas and uses DuckDB with `iceberg` and `cache_httpfs`. It restricts inputs to the caller's authorized rows before running SQL, then rechecks before returning. The bundle compiler derives GraphQL query types from SQL results, mutation inputs from JSON Schema, and subscription types from dataset/view schemas. Nested and VARIANT values are returned through JSON scalars.

Mutations enter Kafka through authenticated ingestion and return acceptance receipts. Subscriptions use shared Kafka consumers and per-emission authorization over `graphql-transport-ws` with Reactor Flux. Private catalog and object-store credentials are not exposed to callers.

## Application definitions

The harness compiles JSON Schema, Flink SQL, serving SQL, and operation bindings into a versioned bundle. It creates endpoint definitions, topic bindings, typed source adapters, table schemas, SQL job definitions, and GraphQL definitions. New documents do not need domain entity fields; their permission resource binding is supplied separately.

Each input dataset has one persistence owner. Jobs use explicit source bindings and application-defined SQL. Scope rules constrain transformations so an application cannot relabel private inputs as a public output. Tables are append-only in the current job writer; updating views require an explicit changelog contract.

The reference configuration includes temporal measurement SQL, media metadata, and example connector datasets. Those are application definitions, not special storage kinds in the processor. No fixed aggregation DSL or node/edge projection runs alongside them.

## Task execution

AX owns task/workspace coordination, and Substrate runs isolated worker images. Applications define task behavior and delegate permissioned API access or scoped callbacks. Workers can ingest ordinary run events through configured endpoints.

Builder tasks can run the same `cg` compiler used by developers and CI. Release automation retains deployment credentials. The shipped workplace chat application uses a trusted model coordinator and an AX retrieval worker; it is one consumer of the platform. See [AX execution](ax.md).

## Deployment and operations

Flink runs as native Kubernetes JobManager/TaskManager Deployments, without a custom operator. Incremental RocksDB checkpoints, one concurrent checkpoint, a minimum pause, retained externalized checkpoints, and savepoints use private object storage. The replacement job has a fresh HA identity and does not restore the removed job's state.

The independent control plane is read-only and observes Kubernetes, Flink REST, configured APIs/queries, and Prometheus metrics. Stateless APIs support readiness and draining. Stateful data continuity across application releases is a separate concern from pod availability. Local single-node PostgreSQL and object storage remain single points of failure.

The previous fixed-table model is removed. There is no migration runner or old-table compatibility adapter in the replacement processor.
