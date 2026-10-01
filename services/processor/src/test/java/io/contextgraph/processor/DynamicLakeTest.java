package io.contextgraph.processor;

import java.util.*;
import org.apache.flink.util.Collector;
import org.apache.iceberg.flink.sink.dynamic.DynamicRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class DynamicLakeTest {
  static <T> Collector<T> collector(List<T> values) {
    return new Collector<>() {
      public void collect(T value) { values.add(value); }
      public void close() {}
    };
  }
  static LakeRecord record(String topic, byte[] value) throws Exception {
    var input = new ConsumerRecord<byte[], byte[]>(topic, 2, 42L, new byte[]{0, -1}, value);
    input.headers().add("duplicate", new byte[]{1}).add("duplicate", null);
    var records = new ArrayList<LakeRecord>();
    new LakeRecord.Deserializer().deserialize(input, collector(records));
    return records.getFirst();
  }
  @Test void preservesBinaryTombstonesAndDuplicateHeaders() throws Exception {
    for (byte[] value : new byte[][]{null, new byte[0], new byte[]{-1, 0, 1}}) {
      var records = new ArrayList<DynamicRecord>();
      new LakeRecordGenerator("bronze", 2).generate(record("a.b", value), collector(records));
      var row = records.getFirst().rowData();
      assertArrayEquals(new byte[]{0, -1}, row.getBinary(5));
      if (value == null) assertTrue(row.isNullAt(6)); else assertArrayEquals(value, row.getBinary(6));
      assertEquals(value == null, row.getBoolean(8));
      assertEquals(42L, row.getLong(2));
      var headers = row.getArray(7);
      assertEquals(2, headers.size());
      assertEquals("duplicate", headers.getRow(0, 2).getString(0).toString());
      assertTrue(headers.getRow(1, 2).isNullAt(1));
    }
  }
  @Test void routingIsCollisionFree() {
    var topics = List.of("a.b", "a_b", "a-b", "A", "a");
    assertEquals(topics.size(), topics.stream().map(LakeRecordGenerator::tableName).distinct().count());
  }
  static LakeRecord cdc(String key, String event) throws Exception {
    var record = record("postgres.public.accounts", event == null ? null : event.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    record.key = key == null ? null : key.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    return record;
  }
  static List<DynamicRecord> convert(LakeRecord record) throws Exception {
    var result = new ArrayList<DynamicRecord>();
    new LakeRecordGenerator("bronze", 2, "^postgres\\..*").generate(record, collector(result));
    return result;
  }
  @Test void canonicalizesCompositeKeysAndReadsSchemaEnvelopes() throws Exception {
    var plain = convert(cdc("{\"b\":2,\"a\":1}", "{\"op\":\"r\",\"after\":{\"id\":1}}"));
    var wrapped = convert(cdc("{\"schema\":{},\"payload\":{\"a\":1,\"b\":2}}",
        "{\"schema\":{},\"payload\":{\"op\":\"u\",\"after\":{\"id\":1,\"new_column\":true}}}"));
    assertEquals(plain.getFirst().rowData().getString(0), wrapped.getFirst().rowData().getString(0));
    assertTrue(wrapped.getFirst().upsertMode());
    assertEquals(Set.of("cdc_key"), wrapped.getFirst().equalityFields());
    assertEquals(org.apache.iceberg.DistributionMode.HASH, wrapped.getFirst().distributionMode());
    assertTrue(convert(cdc("{\"id\":1}", null)).isEmpty());
    assertEquals(org.apache.flink.types.RowKind.DELETE,
        convert(cdc("{\"id\":1}", "{\"op\":\"d\",\"after\":null}")).getFirst().rowData().getRowKind());
  }
  @Test void rejectsUnusableCdcWithoutSilentlyLosingChanges() {
    for (String key : new String[]{null, "{}", "null", "{\"id\":null}"}) {
      assertThrows(IllegalArgumentException.class,
          () -> convert(cdc(key, "{\"op\":\"c\",\"after\":{\"id\":1}}")));
    }
    for (String event : new String[]{"{\"op\":\"t\"}", "{\"op\":\"u\",\"after\":null}", "{}"}) {
      assertThrows(IllegalArgumentException.class, () -> convert(cdc("{\"id\":1}", event)));
    }
  }
  @Test void rejectsUnavailableToastAndPreservesDecimalPrecision() throws Exception {
    for (String placeholder : List.of("__debezium_unavailable_value",
        java.util.Base64.getEncoder().encodeToString("__debezium_unavailable_value".getBytes(java.nio.charset.StandardCharsets.UTF_8)))) {
      assertThrows(IllegalArgumentException.class, () -> convert(cdc("{\"id\":1}",
          "{\"op\":\"u\",\"after\":{\"nested\":[\"" + placeholder + "\"]}}")));
    }
    var row = convert(cdc("{\"id\":1}",
        "{\"op\":\"u\",\"after\":{\"amount\":123456789.123456789123456789}}"))
        .getFirst().rowData();
    assertTrue(row.getString(1).toString().contains("123456789.123456789123456789"));
  }
  static void write(org.apache.iceberg.flink.CatalogLoader loader, LakeRecord... records) throws Exception {
    var env = org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);
    var input = env.fromData(records).setParallelism(1);
    org.apache.iceberg.flink.sink.dynamic.DynamicIcebergSink.forInput(input)
        .generator(new LakeRecordGenerator("bronze", 2, "^postgres\\..*")).catalogLoader(loader)
        .writeParallelism(2).immediateTableUpdate(true).append();
    env.execute("dynamic-lake-test");
  }
  @Test void commitsMultipleTablesAndAppliesUpdatesAndDeletesAcrossCommits(@TempDir java.nio.file.Path warehouse) throws Exception {
    var loader = org.apache.iceberg.flink.CatalogLoader.hadoop("test", new org.apache.hadoop.conf.Configuration(),
        Map.of("warehouse", warehouse.toUri().toString()));
    write(loader,
        cdc("{\"id\":1}", "{\"op\":\"r\",\"after\":{\"id\":1,\"name\":\"old\"}}"),
        cdc("{\"id\":2}", "{\"op\":\"c\",\"after\":{\"id\":2}}"),
        record("events", new byte[]{1, 2}));
    write(loader,
        cdc("{\"id\":1}", "{\"op\":\"u\",\"after\":{\"id\":1,\"name\":\"new\",\"added\":true}}"),
        cdc("{\"id\":2}", "{\"op\":\"d\",\"after\":null}"),
        cdc("{\"id\":2}", null),
        cdc("{\"id\":3}", "{\"op\":\"c\",\"after\":{\"id\":3}}"),
        cdc("{\"id\":3}", "{\"op\":\"d\",\"after\":null}"),
        record("events", null), record("new.topic", new byte[]{-1}));
    var catalog = loader.loadCatalog();
    try {
      var table = catalog.loadTable(org.apache.iceberg.catalog.TableIdentifier.of("bronze",
          LakeRecordGenerator.tableName("postgres.public.accounts")));
      var rows = new ArrayList<org.apache.iceberg.data.Record>();
      try (var scan = org.apache.iceberg.data.IcebergGenerics.read(table).build()) { scan.forEach(rows::add); }
      assertEquals(1, rows.size());
      assertEquals("{\"id\":1}", rows.getFirst().getField("cdc_key").toString());
      assertEquals("{\"id\":1,\"name\":\"new\",\"added\":true}", rows.getFirst().getField("row_json").toString());
      for (var topic : List.of("events", "new.topic")) {
        var raw = catalog.loadTable(org.apache.iceberg.catalog.TableIdentifier.of("bronze", LakeRecordGenerator.tableName(topic)));
        assertEquals(topic.equals("events") ? "2" : "1", raw.currentSnapshot().summary().get("total-records"));
      }
    } finally { ((AutoCloseable) catalog).close(); }
  }
}
