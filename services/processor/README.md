# Schema-defined Flink processing

`SqlBundleJob` consumes configured Kafka inputs, validates their JSON Schemas, and writes typed Iceberg datasets plus Kafka changes. `JsonSchemaRows` maps nested fields and native VARIANT; `_json_remainder` retains unmapped keys. Optional SQL views define further transformations and temporal windows.

There are no fixed storage kinds, mandatory node/edge outputs, or aggregation DSL. `GenericIceberg` writes nested structs, arrays, maps, decimals, and VARIANT to Iceberg v3. `IcebergTables` supplies the shared catalog connection only. `RuntimeSupport` supplies authenticated Kafka sink bindings.

See [schema mapping](../../docs/json-schema-storage.md) and [application bundles](../harness/README.md). Native Kubernetes resources run the jobs, with incremental checkpoints and object-storage recovery state.
