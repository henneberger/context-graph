package io.contextgraph.processor;

import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import org.apache.flink.table.data.*;
import org.apache.flink.util.Collector;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.sink.dynamic.*;
import org.apache.iceberg.types.Types;

/** A stable bronze schema accepts every Kafka encoding without guessing payload types. */
public final class LakeRecordGenerator implements DynamicRecordGenerator<LakeRecord> {
  static final Schema SCHEMA = new Schema(
      Types.NestedField.required(1, "topic", Types.StringType.get()),
      Types.NestedField.required(2, "kafka_partition", Types.IntegerType.get()),
      Types.NestedField.required(3, "kafka_offset", Types.LongType.get()),
      Types.NestedField.required(4, "kafka_timestamp_ms", Types.LongType.get()),
      Types.NestedField.required(5, "timestamp_type", Types.StringType.get()),
      Types.NestedField.optional(6, "key_bytes", Types.BinaryType.get()),
      Types.NestedField.optional(7, "value_bytes", Types.BinaryType.get()),
      Types.NestedField.required(8, "headers", Types.ListType.ofRequired(9, Types.StructType.of(
          Types.NestedField.required(10, "name", Types.StringType.get()),
          Types.NestedField.optional(11, "value", Types.BinaryType.get())))),
      Types.NestedField.required(12, "tombstone", Types.BooleanType.get()));
  private final String namespace;
  private final int parallelism;
  private final java.util.regex.Pattern cdcTopics;
  private final PostgresCdcGenerator cdc;

  public LakeRecordGenerator(String namespace, int parallelism) {
    this(namespace, parallelism, "(?!)");
  }

  public LakeRecordGenerator(String namespace, int parallelism, String cdcTopicPattern) {
    this.namespace = namespace; this.parallelism = parallelism;
    this.cdcTopics = java.util.regex.Pattern.compile(cdcTopicPattern);
    this.cdc = new PostgresCdcGenerator(namespace, parallelism);
  }

  // Reversible, case-safe encoding: a.b, a_b, a-b, A and a never share a table.
  public static String tableName(String topic) {
    if (topic == null || topic.isEmpty()) throw new IllegalArgumentException("Topic is required");
    return "topic_" + HexFormat.of().formatHex(topic.getBytes(StandardCharsets.UTF_8));
  }

  @Override public void generate(LakeRecord r, Collector<DynamicRecord> out) throws Exception {
    if (cdcTopics.matcher(r.topic).matches()) { cdc.generate(r, out); return; }
    Object[] headers = new Object[r.headerNames.length];
    for (int i = 0; i < headers.length; i++)
      headers[i] = GenericRowData.of(StringData.fromString(r.headerNames[i]), r.headerValues[i]);
    var row = GenericRowData.of(StringData.fromString(r.topic), r.partition, r.offset,
        r.timestamp, StringData.fromString(r.timestampType), r.key, r.value,
        new GenericArrayData(headers), r.value == null);
    out.collect(new DynamicRecord(TableIdentifier.of(namespace, tableName(r.topic)), "main", SCHEMA,
        row, PartitionSpec.unpartitioned(), DistributionMode.NONE, parallelism));
  }
}
