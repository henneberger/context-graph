package io.contextgraph.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Set;
import org.apache.flink.table.data.*;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.sink.dynamic.*;
import org.apache.iceberg.types.Types;

/** Debezium JSON current-state projection. Primary keys come from Kafka keys. */
public final class PostgresCdcGenerator implements DynamicRecordGenerator<LakeRecord> {
  private static final ObjectMapper JSON = new ObjectMapper()
      .enable(com.fasterxml.jackson.databind.DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
      .enable(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
      .enable(com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
  private static final String UNAVAILABLE = "__debezium_unavailable_value";
  private static final String UNAVAILABLE_BINARY = java.util.Base64.getEncoder()
      .encodeToString(UNAVAILABLE.getBytes(java.nio.charset.StandardCharsets.UTF_8));
  static final Schema SCHEMA = new Schema(java.util.List.of(
      Types.NestedField.required(1, "cdc_key", Types.StringType.get()),
      Types.NestedField.optional(2, "row_json", Types.StringType.get()),
      Types.NestedField.required(3, "topic", Types.StringType.get()),
      Types.NestedField.required(4, "kafka_partition", Types.IntegerType.get()),
      Types.NestedField.required(5, "kafka_offset", Types.LongType.get())), Set.of(1));
  private final String namespace;
  private final int parallelism;
  public PostgresCdcGenerator(String namespace, int parallelism) {
    this.namespace = namespace; this.parallelism = parallelism;
  }
  @Override public void generate(LakeRecord r, Collector<DynamicRecord> out) throws Exception {
    // Debezium emits the delete event before the Kafka compaction tombstone.
    if (r.value == null) return;
    if (r.key == null) throw new IllegalArgumentException("CDC requires a primary key: " + r.topic);
    JsonNode key = unwrap(JSON.readTree(r.key));
    if (key == null || !key.isObject() || key.isEmpty())
      throw new IllegalArgumentException("CDC requires a nonempty JSON primary key: " + r.topic);
    for (var field : key) if (field.isNull()) throw new IllegalArgumentException("Null CDC primary key");
    JsonNode event = unwrap(JSON.readTree(r.value));
    if (event == null || !event.isObject()) throw new IllegalArgumentException("Expected Debezium JSON envelope");
    String op = event.path("op").asText();
    if (!Set.of("r", "c", "u", "d").contains(op))
      throw new IllegalArgumentException("Unsupported CDC operation: " + op + " on " + r.topic);
    JsonNode after = event.path("after");
    if (!op.equals("d") && !after.isObject()) throw new IllegalArgumentException("CDC after row is missing");
    if (!op.equals("d") && containsUnavailable(after))
      throw new IllegalArgumentException("CDC row contains unavailable TOAST values; configure REPLICA IDENTITY FULL: " + r.topic);
    var row = GenericRowData.of(StringData.fromString(canonical(key).toString()),
        op.equals("d") ? null : StringData.fromString(after.toString()),
        StringData.fromString(r.topic), r.partition, r.offset);
    row.setRowKind(op.equals("d") ? RowKind.DELETE : RowKind.INSERT);
    var dynamic = new DynamicRecord(TableIdentifier.of(namespace, LakeRecordGenerator.tableName(r.topic)),
        "main", SCHEMA, row, PartitionSpec.unpartitioned(), DistributionMode.HASH, parallelism);
    dynamic.setUpsertMode(true); dynamic.setEqualityFields(Set.of("cdc_key"));
    out.collect(dynamic);
  }
  private static boolean containsUnavailable(JsonNode node) {
    if (node.isTextual()) return UNAVAILABLE.equals(node.textValue()) || UNAVAILABLE_BINARY.equals(node.textValue());
    if (node.isContainerNode()) for (JsonNode child : node) if (containsUnavailable(child)) return true;
    return false;
  }
  private static JsonNode unwrap(JsonNode value) {
    return value != null && value.has("schema") && value.has("payload") ? value.get("payload") : value;
  }
  private static JsonNode canonical(JsonNode value) {
    if (value.isObject()) {
      var result = JSON.createObjectNode();
      var names = new java.util.TreeSet<String>(); value.fieldNames().forEachRemaining(names::add);
      for (String name : names) result.set(name, canonical(value.get(name)));
      return result;
    }
    if (value.isArray()) {
      var result = JSON.createArrayNode(); value.forEach(v -> result.add(canonical(v))); return result;
    }
    return value;
  }
}
