# Architecture diagrams

Three views explain different contracts of the platform. The component names refer to services and code in this repository. Slack, GitHub, and chat are examples built on those contracts.

## Data paths

![Platform data paths](context-graph-overview.svg)

Read the top row left to right: schema-validated input becomes Kafka events, Flink jobs produce schema-defined datasets and SQL views, Iceberg retains them, and SQL-backed APIs serve authorized views. The live lane reaches clients through Kafka subscriptions without waiting for a lakehouse query. The media lane retains bytes in object storage and sends only metadata through the event pipeline.

Polaris manages catalog metadata and vends storage credentials to service identities. It does not replace SpiceDB or grant end users direct warehouse access. Native Kubernetes deployments run the Flink jobs; incremental RocksDB checkpoints and savepoints use a separate private recovery bucket.

Temporal source adapters, synthetic data generators, and application producers all enter through ingestion contracts. The read-only control plane observes Kubernetes, Flink REST, configured APIs/queries, and metrics. PostgreSQL supports service metadata; application data lives in schema-defined Iceberg tables.

## Execution and extension

![AX tasks and application builders](ax-task-execution.svg)

The two columns distinguish using deployed APIs from creating new application definitions. Both can run as AX tasks on Substrate. Builders compile bundles with `cg`; the release workflow holds the privileges to provision resources. Ordinary task workers receive delegated API access or scoped callbacks.

The bottom path identifies the shipped chat implementation: its trusted coordinator handles model calls and evidence, while its AX worker executes retrieval. Other applications define their own task behavior.

## Permission boundaries

![Access enforcement](permission-boundaries.svg)

Private files may contain entities with different permissions. The query service materializes authorized inputs **before** configured SQL executes, then rechecks before results leave. This boundary also applies to derived views: an aggregate must not expose unauthorized source rows. Subscriptions, media delivery, and task retrieval apply the same caller context through their own access paths.

Solid connectors show processing or delivery sequence. The dashed connector shows authorization decisions. These views describe contracts, not a pod/network topology or a guarantee of atomic commits across Kafka and Iceberg.

## Editing and exports

Edit [`generate_architecture.py`](generate_architecture.py), then run:

```sh
python3 docs/diagrams/generate_architecture.py
```

Python 3 and `rsvg-convert` (librsvg) are required. The script emits self-contained SVGs, PNG renderings, a PDF overview, and an HTML viewer. README embeds the SVGs directly; the PNGs are deterministic exports of the same diagrams, not generated artwork. Text remains editable and project marks are embedded so GitHub rendering does not depend on external image requests.

Project marks are vendored in [`logos/`](logos/SOURCES.md), with source URLs and attribution. AX and services without a vendored mark use their names in plain text. Third-party marks retain their owners' rights and do not imply endorsement.

Existing `context-graph-architecture.*` links resolve to the current overview for compatibility.
