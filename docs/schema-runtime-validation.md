# Schema runtime validation

The storage replacement uses JSON Schema fields and native `_json_remainder VARIANT`, with no node/edge projection or data migration.

Validation on the local Kubernetes platform includes an authenticated Kafka → Flink → Iceberg → DuckDB → GraphQL round trip preserving a nested object, mixed array, boolean, and JSON null. Schema-invalid input was rejected. A user without the resource grant could not retrieve the row. The reference temporal SQL query returned the expected count and sum. Flink completed checkpoints in object storage. Replayed connector documents were searchable in the `knowledge` workspace. The final smoke output is recorded in [sanitized evidence](evidence/schema-runtime.json).

Reproduce that check with `scripts/schema-smoke.py --context YOUR_CONTEXT`, using the local development fixture credentials. The script submits a fresh marker and waits for that exact record; an empty or stale query result cannot pass.

The processor test suite additionally writes native VARIANT and nested structs through a real Flink Iceberg sink, reads Parquet back, and queries the table through DuckDB's Iceberg extension. It covers root arrays/scalars/null, local references, schema rejection, mixed values, and numeric precision. Query tests cover JDBC normalization and access checks. Both bundled application definitions compile, including persistence with no SQL transformation views. Native Kubernetes rendering was checked without the Flink operator.

These checks do not establish zero-downtime stateful upgrades, scale capacity, or a complete media/security regression run. Earlier broad security and media evidence remains historical until rerun against this storage revision.
