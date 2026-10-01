# Dynamic lake ingestion

`DynamicLakeJob` uses the Flink DataStream API and Iceberg's `DynamicIcebergSink`.
One regex Kafka subscription feeds one dynamic sink. Matching topics discovered
while the job runs create their own Iceberg tables on the first record. No SQL,
per-topic DDL, or schema files are needed.

Topics matching `cdcTopicPattern` become **current-row Postgres tables**.
Debezium snapshot (`r`), insert (`c`), and update (`u`) events upsert by the Kafka
record's primary key; delete (`d`) events issue Iceberg equality deletes. A
compaction tombstone following a delete produces no additional row. Composite
keys are canonicalized to make JSON field order irrelevant. HASH distribution
keeps writes for a key on the same sink writer.

CDC tables contain `cdc_key` (canonical JSON primary key), `row_json` (the latest
Debezium `after` object), `topic`, `kafka_partition`, and `kafka_offset`. The row
stays in JSON, including newly added source fields; this version does not project
source columns into individually typed Iceberg columns or resolve Schema Registry
schemas. Decimal numbers are parsed without converting through floating point.
Debezium logical types retain their JSON converter representations.

Other topics are append-only archives. Their tables store `topic`,
`kafka_partition`, `kafka_offset`, `kafka_timestamp_ms`, `timestamp_type`,
`key_bytes`, `value_bytes`, ordered `headers` (including duplicate names and null
values), and `tombstone`. Arbitrary JSON, Avro, Protobuf and binary messages are
preserved as bytes. Schema Registry wire IDs remain in those bytes. Payload schema
changes need no lake schema migration. Both routes use the same dynamic sink.

Table names are `topic_` followed by lowercase hexadecimal UTF-8 topic bytes.
This reversible mapping avoids punctuation and case collisions. The original
name is also a column. Use a separate namespace and job identity for each Kafka
cluster. Names can reach 504 characters for maximum-length Kafka topic names;
verify identifier limits if substituting a different catalog.

## Run

Build with Java 21 from the repository root:

```sh
mvn -pl services/processor -am package -DskipTests
flink run -c io.contextgraph.processor.DynamicLakeJob \
  services/processor/target/processor.jar config/lake.yaml
```

Set `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_CLIENT_CONFIG` (the existing mandatory
SASL_SSL properties file), and `CHECKPOINT_URI`. The existing `POLARIS_URI`,
`POLARIS_WAREHOUSE`, and `POLARIS_CREDENTIAL` bindings supply the REST catalog
and vended storage credentials. Without Polaris, `WAREHOUSE_URI` selects a
Hadoop catalog for local tests. The catalog principal needs namespace/table
creation and write permissions. Raw tables belong in a restricted bronze
namespace; they do not carry the query service's enforced row authorization.

Edit `config/lake.yaml` to select topics. `.*` selects every visible topic;
the example selects application and Postgres topics. Kafka ACLs must permit
matching topics and the configured consumer group. `cdcTopicPattern` selects a
subset of subscribed topics; omit it to archive every topic as raw records. Do
not change an existing topic between raw and CDC modes within the same namespace:
use a new namespace/job and backfill to avoid mixing incompatible table semantics. New topics/partitions are
checked every `topicDiscoveryIntervalMs`. Empty topics have no table until their
first record. Run this as a separate Flink application using the existing
processor image, passing `standalone-job --job-classname
io.contextgraph.processor.DynamicLakeJob /app/config/lake.yaml`; mount the config
and credentials into both JobManager and TaskManagers. The existing SQL bundle
application remains available for downstream transformations.

Offsets start at the earliest retained record for fresh state; recovery uses
Flink checkpoint/savepoint state. Restore state when restarting: starting a new
job without it intentionally replays records and can duplicate lake history.
Commits follow successful checkpoints. Kafka uses `read_committed`. Raw archives
do not deduplicate upstream producer retries. CDC applies events in Kafka order,
without LSN-based suppression of stale upstream replays. `(topic, kafka_partition,
kafka_offset)` identifies a Kafka record. Keep checkpoint storage durable and
monitor successful Iceberg commits as well as Flink checkpoints.

## PostgreSQL CDC

Use Kafka Connect with the Debezium PostgreSQL connector installed. The example
at `config/connectors/postgres-cdc.example.json` emits one topic per source table,
`postgres.<schema>.<table>`, so each gets a distinct Iceberg table automatically.
The Flink job consumes CDC from Kafka; it does not connect directly to Postgres.

Configure PostgreSQL logical replication (`wal_level=logical`, adequate
replication slots and WAL senders), a replication account with snapshot SELECT
privileges, and a publication named `context_lake` covering the intended tables.
The example requires that publication to exist. Mount database credentials and
CA into Connect and enable its `file` config provider for the password reference.
Configure the Connect worker's authenticated Kafka connection separately. Submit
the example to the worker's connectors endpoint after replacing host/database
settings. No Connect worker or database changes are deployed by this code.

Keep Debezium JSON envelopes intact (schemas enabled or disabled are accepted).
CDC topics must have non-null JSON primary keys and one source table per topic.
The example uses the JSON converters with schemas enabled. Avro/Protobuf CDC
messages can be archived through the raw route but are not decoded for upserts.
Route heartbeat and transaction topics through the raw path, not the CDC pattern.

Configure `ALTER TABLE <table> REPLICA IDENTITY FULL` for captured tables so
unchanged TOAST values are present in updates. The job rejects Debezium's default
unavailable-value placeholder (including its binary/base64 form); keep the example
placeholder setting unchanged. That reserved string cannot be used as ordinary
row data in this profile. Missing keys, invalid envelopes, unsupported operations
(including truncate), and incomplete updates stop the job instead of being
silently skipped. Repair the source/configuration and restore from a checkpoint.
This job does not implement table truncation; a truncation requires rebuilding the
affected current-state table from a fresh snapshot.

Keep all events for a primary key on the same Kafka partition, preserve their
order, and keep the partition count and key encoding stable. Changing a primary
key is handled by Debezium's old-key delete and new-key create events. Changes
across different tables become visible in separate Iceberg commits; there is no
cross-table database transaction guarantee. Provision a single owning job for each
topic/table mapping. Initial snapshot coverage and sufficient Kafka retention are
required for a complete current-state table. Replaying only retained updates into
an existing table is not a substitute for rebuilding it after a lost checkpoint.

Adding tables to the publication/connector may require a Debezium incremental
snapshot to backfill existing rows; Kafka topic discovery alone cannot backfill
Postgres. Monitor replication-slot WAL retention while Connect is unavailable.

## Validation

`DynamicLakeTest` runs a local Flink MiniCluster with the repository's Flink 2.3.0
and Iceberg 1.11.0 dependencies. It creates multiple tables with two sink writers,
then runs a second ingestion and reads Iceberg data back to verify updates,
deletes of prior rows, same-commit insert/delete pairs, tombstones, and a newly
introduced topic. Unit checks cover binary transport, duplicate headers, composite
keys, schema envelopes, unavailable TOAST values, and decimal precision.

```sh
mvn -pl services/processor -am test -Dtest=DynamicLakeTest \
  -Dsurefire.failIfNoSpecifiedTests=false
```

These tests do not run a live Postgres/Debezium/Kafka/Polaris stack or prove recovery
after injected failures. Verify topic discovery, checkpoint restore, connector
permissions, and catalog credential vending in the target deployment before rollout.

References: [Flink dynamic sink walkthrough](https://flink.apache.org/2025/11/11/from-stream-to-lakehouse-kafka-ingestion-with-the-flink-dynamic-iceberg-sink/),
[Iceberg DataStream sink](https://iceberg.apache.org/docs/latest/flink-writes/#flink-dynamic-iceberg-sink),
[Debezium PostgreSQL connector](https://debezium.io/documentation/reference/stable/connectors/postgresql.html).
