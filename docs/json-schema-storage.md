# JSON Schema → Flink → Iceberg

JSON Schema is the input contract. Root arrays, scalars, and null are accepted when the schema permits them; without declared object fields, the whole value is stored in `_json_remainder VARIANT`. The platform does not define node types, relationships, or an application ontology. Each endpoint has its own schema and Kafka input topic. A schema adapter validates each payload again in Flink, produces relational fields, and writes the input dataset to Iceberg. Applications define additional SQL views when they need transformations.

## Structured fields and the remainder

`_json_remainder` is a reserved **native VARIANT** column. It contains precisely the keys that are not declared in the current object's `properties`. It is not a string containing JSON and is not a duplicate of the entire input document.

For this schema:

```json
{
  "type": "object",
  "properties": {
    "label": {"type": "string"},
    "profile": {
      "type": "object",
      "properties": {"active": {"type": "boolean"}}
    }
  },
  "additionalProperties": true
}
```

this input:

```json
{
  "label": "sample",
  "profile": {"active": true, "region": "west"},
  "observations": [1, "two", null, {"ok": true}]
}
```

becomes:

```text
label                     STRING  "sample"
profile                   ROW
  active                  BOOLEAN true
  _json_remainder         VARIANT {"region":"west"}
_json_remainder           VARIANT {"observations":[1,"two",null,{"ok":true}]}
```

Every structured object gets its own remainder, including objects inside arrays. An open-ended object without declared properties is a VARIANT value itself. Heterogeneous unions and positional tuple arrays also remain VARIANT. The full JSON Schema still validates them: a remainder does not bypass `additionalProperties: false`, patterns, required fields, or other validation rules.

| JSON Schema | Flink / Iceberg representation |
| --- | --- |
| String / boolean | STRING / BOOLEAN |
| Integer with explicit bounds inside signed 64-bit range | BIGINT / LONG |
| Number with explicit bounds and decimal `multipleOf`, fitting precision 38 | DECIMAL |
| Unbounded numeric schema | VARIANT, preserving representable numeric precision |
| Object with declared properties | ROW / STRUCT, plus `_json_remainder` |
| Homogeneous array | ARRAY / LIST of the mapped item type |
| Open object, unconstrained value, heterogeneous union | VARIANT |
| Undeclared keys | `_json_remainder` VARIANT object |

Numbers are parsed as decimals before mapping. Values exceeding native VARIANT decimal precision/scale are rejected through the error path; the adapter never rounds them silently to floating point. Native storage limits still apply. Structured nullable columns use SQL NULL; this does not distinguish a missing optional field from an explicitly null structured field. Within VARIANT objects, missing keys and JSON null remain distinct.

The schema adapter supports acyclic local references for structured fields. `_json_remainder` cannot be declared as a user field. Transport columns are reserved: `workspace_id`, `resource_id`, `entity_id`, `event_id`, `event_time`, and `ingested_at`. A collision fails compilation rather than overwriting user data.

## Authorization is transport metadata

New bundle endpoints take their access resource from the `X-Resource-Key` header, separately from the JSON document. GraphQL mutations take a separate `resourceKey` argument. The server verifies the caller's permission and stamps the envelope. A document does not need an `entityId` property.

`entity_id` is currently the internal column name for that authorization resource key. It does not cause an entity table or relationship to be created. SQL output scope columns must remain unchanged, and resource-local aggregates must group by them.

Existing example endpoints explicitly bind their resource key using a configured JSON Pointer. That is an endpoint definition, not inferred document semantics.

## Runtime and APIs

The source uses the standard Kafka connector with string deserialization followed by the schema adapter. A connector fork is unnecessary for this implementation. Native Flink VARIANT values are written through the Iceberg adapter to format-version 3 tables. Nested structs, lists, decimals, and VARIANT values also survive SQL outputs and Kafka change records.

The serving API uses DuckDB with `iceberg` and `cache_httpfs`. JDBC nested values are normalized to structured maps and lists. GraphQL exposes nested and VARIANT results as JSON scalars; primitive fields retain their derived scalar types. Open-ended mutation inputs use a JSON scalar so GraphQL does not discard/reject keys allowed by JSON Schema. Flink revalidates the input regardless of the entry path.

Standalone platform sources can explicitly specify their workspace; bundle sources inherit the bundle workspace. A source rejects envelopes from other workspaces.

Each input has one owning persistence job. Additional jobs and datasets are explicit bundle definitions. There are no mandatory node/edge sinks and no fixed aggregation DSL. Temporal computations are SQL views.

## Example and verification

[`examples/schema-data`](../examples/schema-data) contains an input schema with nested objects, arrays, unconstrained attributes, and an open remainder; its bundle persists records without requiring a transformation view. Query, mutation, and subscription APIs derive from that dataset.

`JsonSchemaRowsTest` covers structured mapping, nested remainders, schema rejection, heterogeneous values, numeric precision, and a real Flink write to Iceberg followed by native readback. `VariantQueryTest` checks structured DuckDB JDBC results. The optional Kafka integration test checks committed output and the error topic against a real broker.

This is a replacement storage contract. There is no converter for the removed fixed event/node/edge tables and no attempt to restore their Flink state into the new job.
