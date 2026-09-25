# Governed lakehouse and SQL serving

Application datasets are schema-derived Iceberg v3 tables stored in private RustFS object storage. Declared fields retain relational types; open-ended values and `_json_remainder` use native VARIANT. See [schema mapping](json-schema-storage.md).

## Catalog and storage identities

Polaris is the Iceberg REST catalog. Flink writes with a service identity; DuckDB reads through a separate service identity. Polaris vends temporary object-storage credentials for table access. The processor cannot become the query user, and end users never receive credentials to files containing mixed permissions.

The warehouse, media, and Flink recovery state have separate private buckets. Ingestion's storage identity is scoped to media. Recovery credentials are separate from catalog-vended warehouse access. Catalog credentials, storage identities, and certificate material live in Kubernetes Secrets.

The serving API attaches Polaris with its private identity and uses DuckDB's `iceberg` and `cache_httpfs` extensions. Object caching is private infrastructure and does not cache authorization decisions.

## Permissioned query boundary

The API authenticates the caller and workspace, discovers the registered table schema, validates resource labels, and materializes only authorized source rows. Configured SQL runs after this step. This ensures unauthorized contributions do not enter joins, aggregates, ranking, or limits.

Federated queries authorize each declared input separately and copy those rows into a credential-free DuckDB database. Nested structs, arrays, decimals, and VARIANT values retain their types. External access is disabled before the configured SQL runs. Membership and contributing resources are checked again before returning results.

Query source mappings and SQL are trusted application definitions. Caller values use bound parameters. Schema discovery exposes registered schemas, not catalog tokens or physical file locations. Each source may resolve a separate committed snapshot; cross-table reads are not a globally atomic snapshot.

The current request executor has bounded input rows/resources and memory. Further work on output bounds, cancellation, and shared indexing is tracked separately; private file access must not be substituted for the permissioned boundary.

## Configuration and operations

`config/jobs.yaml` is the reference application's schema/SQL job definition. `config/queries.yaml` contains its configured SQL operations. Application bundles generate their own definitions, schemas, topics, catalog roles, and tables.

Native Flink JobManager/TaskManager Deployments use incremental RocksDB checkpoints and object-storage savepoints.

To reset a local development deployment, the explicit reset command stops writers, purges application tables, clears input/output streams, applies the replacement runtime, and makes example connectors scrape again:

```sh
python scripts/reset-schema-runtime.py --context YOUR_CONTEXT --node YOUR_NODE --reset-data
```

This command is destructive. It retains identities and access grants, and does not convert or copy old application rows. Image builds must already exist. It temporarily enables Polaris table purging for the reset and restores the previous catalog properties afterward.

Polaris database state and object storage need coordinated backup/recovery procedures for a production deployment. The local single-node storage/database profile is not highly available.
