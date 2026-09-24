# Configured Flink processor

Java 21 / Flink **2.3.0**. Entrypoint: `io.contextgraph.processor.ContextGraphJob`.
Build from the repository root: `mvn -f services/processor/pom.xml verify`.
Submit `target/processor.jar` to Flink with `/app/config/jobs.yaml` as the optional argument.
Build the image from the repository root with `docker build -f services/processor/Dockerfile -t context-graph/processor:dev .`.

| Variable | Default |
|---|---|
| CONFIG_PATH | /app/config/jobs.yaml |
| KAFKA_BOOTSTRAP_SERVERS | kafka:9092 |
| KAFKA_CLIENT_CONFIG | Required secret properties file; SASL_SSL and hostname verification enforced |
| WAREHOUSE_URI | file:///data/warehouse |
| CHECKPOINT_URI | file:///data/checkpoints |
| SAVEPOINT_URI | file:///data/savepoints |
| JOB_ID | context-secure-v1 |

Authorization labels are mandatory. The source accepts only schemaVersion 2 envelopes with `security.workspaceId`, `security.resourceId` and `security.subjectId`, and checks `resourceId = SHA256(workspaceId + NUL + entityId)` before assigning source watermarks and before transforming. Missing, legacy or forged labels go to the operational/admin-only error topic. Source machine identity is enforced by Kafka SASL_SSL credentials and ACLs; Flink does not independently grant permissions or call SpiceDB per event.

Every Iceberg/Kafka result carries `workspace_id` and `resource_id`; edges also carry canonical `target_resource_id`. Aggregation keys include workspace and resource so the same entity name in two workspaces cannot merge. Kafka keys use workspace plus resource; transformation configuration cannot remove/remap these labels. The query service must check current SpiceDB permissions before exposing these rows.

Secure migration uses **new** job/group `context-secure-v1`, namespace `context_secure` and `cg.secure.*` topics. Never restore a legacy unlabeled savepoint into the secure job or silently label historical data. Existing legacy tables remain retained but are not secure query sources. Start the secure job fresh after provisioning Kafka ACLs. Subsequent secure job upgrades retain the normal checkpoint/savepoint behavior. `jobs-v2.yaml` is an independently warmed secure version.

Every Flink worker and job manager must see the same warehouse/checkpoint paths. Use separate `JOB_ID`, consumer group, tables, and output topics for parallel blue/green jobs. The Kafka transaction timeout is 15 minutes; broker `transaction.max.timeout.ms` must permit this. In a one-broker development cluster set Kafka transaction-state replication factor and minimum ISR to 1.

`config/jobs.yaml` is read when a job starts. Sources are Kafka JSON strings, so arbitrary object payloads survive without a Kafka connector fork. JSON Schema validates the input envelope, media metadata and every Kafka output, including the common error queue. The input endpoint validates its own payload schema before producing. A failed transformation routes to the error topic; a failed sink fails/restarts the job so input is replayed from the last successful checkpoint.

Transforms use **RFC 6901 JSON Pointer**, not JSONPath. The graph config selects a node type and relationship target; raw payloads remain JSON strings in Iceberg. Each aggregation selects a numeric value and metric label, applies `value * scale + offset`, and emits event-time tumbling-window count/sum/average/min/max. Add multiple aggregation definitions for more windows/projections. Use distinct metric labels for definitions that otherwise have the same entity and window identity. Keep the aggregation `id` stable during compatible job changes: it forms the keyed operator UID. Changing window size, key or state type requires a separately warmed version, not blind restoration of existing state.

Watermarks run inside `KafkaSource`, separately for each Kafka partition, and permit the configured out-of-order delay. This prevents a newer partition from making another partition's buffered records late during restore. Idle partitions stop holding back active partitions. Invalid envelopes neither advance time nor keep an invalid-only partition active; downstream validation still sends them to the error queue. Samples older than a closed window go to that queue; valid raw events still reach history tables. An entirely idle stream does not advance event time, so its final open window waits for a newer event. Windows retain accumulators, not all samples. Daily Iceberg partitions bound scans; schedule Iceberg compaction and snapshot expiration separately as volume grows.

RocksDB incremental checkpoints use durable filesystem storage, one concurrent checkpoint, a minimum pause and retained externalized checkpoints. Savepoints use the configured directory. Checkpoint file merging remains disabled because Flink 2.3 labels it experimental. The chosen defaults avoid persisting in-flight network buffers during backpressure. Both sinks use their checkpoint-transaction mechanisms; Kafka and Iceberg are **separate commits**, with no atomicity spanning both. Browsers reading `read_committed` Kafka see records after a checkpoint, so the default 10-second checkpoint adds live metric latency. Lower checkpoint intervals trade storage/commit pressure for latency; HLS media bypasses this delay.

Compatibility evidence is deliberately narrower than an upstream support statement: Maven publishes Kafka connector `5.0.0-2.2` and Iceberg runtime `1.11.0` for Flink 2.1, while the engine here stays on 2.3. `ProcessorTest` runs real 2.3 MiniClusters, isolated workspace windows and an Iceberg Parquet snapshot commit. With `KAFKA_CLIENT_CONFIG` present it also constructs the complete authenticated Kafka/Iceberg graph. `KafkaIntegrationTest`, enabled with `RUN_KAFKA_IT=true`, requires a SASL_SSL test broker and a secret `KAFKA_CLIENT_CONFIG` with test-only permissions to create/delete/read/write `cg.secure.it-*` topics, use test groups, and write the job transaction prefix. It checks partition watermarks, read_committed outputs, Iceberg commits, canonical savepoints and restoration without duplicate replay. Pre-authorization connector tests passed against Kafka 4.1.1; authenticated deployment evidence must be recorded separately, rather than inferred from those older checks.

Official references: [Flink 2.3 configuration](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/deployment/config/), [Iceberg Flink writes](https://iceberg.apache.org/docs/latest/flink-writes/).
