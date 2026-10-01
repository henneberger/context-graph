# Flink processing

## Dynamic lake ingestion

`DynamicLakeJob` uses the DataStream API and `DynamicIcebergSink` to discover Kafka topics and create one Iceberg table per topic. Generic topics retain original bytes and Kafka metadata. Debezium Postgres CDC topics maintain current rows with primary-key upserts and equality deletes; the latest row is stored in `row_json`.

Configure [config/lake.yaml](../../config/lake.yaml), then build and submit from the repository root with Java 21:

```sh
mvn -pl services/processor -am package -DskipTests
flink run -c io.contextgraph.processor.DynamicLakeJob \
  services/processor/target/processor.jar config/lake.yaml
```

See [lake ingestion setup](LAKE.md) for Kafka authentication, Polaris credentials, checkpoint recovery, the Debezium connector example, and CDC requirements. This path does not use Flink SQL or infer typed payload columns. It runs as a separate application from `SqlBundleJob`.

## Schema-defined application processing

`SqlBundleJob` consumes configured Kafka inputs, validates their JSON Schemas, and writes typed Iceberg datasets plus Kafka changes. `JsonSchemaRows` maps nested fields and native VARIANT; `_json_remainder` retains unmapped keys. Optional SQL views define further transformations and temporal windows.

There are no fixed storage kinds, mandatory node/edge outputs, or aggregation DSL. `GenericIceberg` writes nested structs, arrays, maps, decimals, and VARIANT to Iceberg v3. `IcebergTables` supplies the shared catalog connection only. `RuntimeSupport` supplies authenticated Kafka sink bindings.

See [schema mapping](../../docs/json-schema-storage.md) and [application bundles](../harness/README.md). Native Kubernetes resources run the jobs, with incremental checkpoints and object-storage recovery state.
