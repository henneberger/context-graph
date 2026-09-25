package io.contextgraph.processor;

import org.junit.jupiter.api.Test;
import org.apache.flink.types.Row;
import org.apache.flink.types.variant.Variant;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import static org.junit.jupiter.api.Assertions.*;

class JsonSchemaRowsTest {
  static final String SCHEMA="""
    {"type":"object","required":["title"],"properties":{
      "title":{"type":"string"},
      "count":{"type":"integer","minimum":0,"maximum":100000},
      "details":{"type":"object","properties":{"active":{"type":"boolean"}}},
      "mixed":{},"items":{"type":"array","items":{"type":"object","properties":{"name":{"type":"string"}}}}
    }}
    """;
  @Test void mapsStructuredFieldsAndRetainsNestedRemainders() {
    var adapter=new JsonSchemaRows(SCHEMA);
    var row=adapter.read("""
      {"title":"sample","count":4,"details":{"active":true,"unknown":[1,null,"two"]},
       "mixed":{"a":[false,2.75]},"items":[{"name":"first","extra":{"x":1}}],
       "newField":{"nested":[1,"two",null]},"another":false}
      """);
    assertEquals("sample",row.getField(0));assertEquals(4L,row.getField(1));
    assertEquals(Json.read("{\"unknown\":[1,null,\"two\"]}"),Json.read(((Variant)((Row)row.getField(2)).getField(1)).toJson()));
    assertEquals(Json.read("{\"a\":[false,2.75]}"),Json.read(((Variant)row.getField(3)).toJson()));
    Row item=(Row)((Object[])row.getField(4))[0];assertEquals("first",item.getField(0));
    assertEquals(Json.read("{\"extra\":{\"x\":1}}"),Json.read(((Variant)item.getField(1)).toJson()));
    assertEquals(Json.read("{\"newField\":{\"nested\":[1,\"two\",null]},\"another\":false}"),Json.read(((Variant)row.getField(5)).toJson()));
    var iceberg=FlinkSchemaUtil.convert(adapter.dataType().getLogicalType());
    assertEquals(org.apache.iceberg.types.Type.TypeID.VARIANT,iceberg.asStructType().field("_json_remainder").type().typeId());
  }
  @Test void schemaValidationRunsBeforeMapping() {
    var adapter=new JsonSchemaRows(SCHEMA);
    assertThrows(IllegalArgumentException.class,()->adapter.read("{\"title\":3}"));
    assertThrows(IllegalArgumentException.class,()->adapter.read("{\"title\":\"x\",\"count\":100001}"));
    assertThrows(IllegalArgumentException.class,()->new JsonSchemaRows("{\"type\":\"object\",\"properties\":{\"_json_remainder\":{}}}"));
    var closed=new JsonSchemaRows("{\"type\":\"object\",\"additionalProperties\":false}");
    assertThrows(IllegalArgumentException.class,()->closed.read("{\"unknown\":1}"));
  }
  @Test void heterogenousUnionsAndTupleArraysAreVariant() {
    var adapter=new JsonSchemaRows("""
      {"type":"object","properties":{"value":{"type":["string","object","null"]},
      "tuple":{"type":"array","prefixItems":[{"type":"string"},{"type":"integer"}]}}}
      """);
    var row=adapter.read("{\"value\":null,\"tuple\":[\"x\",1]}");
    assertTrue(((Variant)row.getField(0)).isNull());
    assertEquals("x",((Variant)row.getField(1)).getElement(0).getString());
    assertNull(adapter.read("{}").getField(0));
  }
  @Test void openObjectDoesNotRequireEntityOrDomainFields() {
    var adapter=new JsonSchemaRows("{\"type\":\"object\"}");
    String json="{\"completely\":{\"arbitrary\":[true,42,\"text\",null]}}";
    assertEquals(Json.read(json),Json.read(((Variant)adapter.read(json).getField(0)).toJson()));
  }
  @Test void largeNumbersDoNotSilentlyRoundThroughDouble() {
    var adapter=new JsonSchemaRows("{\"type\":\"object\",\"properties\":{\"value\":{\"type\":\"number\"}}}");
    var row=adapter.read("{\"value\":12345678901234567890.123456789}");
    assertEquals(new java.math.BigDecimal("12345678901234567890.123456789"),((Variant)row.getField(0)).getDecimal());
  }

  @org.junit.jupiter.api.io.TempDir java.nio.file.Path temp;
  @Test void realFlinkWritesNestedFieldsAndVariantToIceberg() throws Exception {
    var mapping=new JsonSchemaRows(SCHEMA);
    var envelope=(com.fasterxml.jackson.databind.node.ObjectNode)Json.read(ProcessorTest.EVENT);
    envelope.set("payload",Json.read("{\"title\":\"sample\",\"details\":{\"active\":true,\"unmapped\":42},\"mixed\":[1,\"two\",null],\"extra\":{\"kept\":true}}"));
    var type=SqlBundleJob.sourceType(mapping);
    var schema=new org.apache.iceberg.Schema(FlinkSchemaUtil.convert(type.getLogicalType()).asStructType().fields());
    var row=SqlBundleJob.input(envelope.toString(),mapping);
    String value=GenericIceberg.encode(row,schema,"workspace-a");
    assertTrue(Json.read(value).path("_json_remainder").path("extra").path("kept").asBoolean());
    var env=org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.getExecutionEnvironment();env.setParallelism(1);
    var warehouse=temp.resolve("warehouse").toUri().toString();
    GenericIceberg.attach(env.fromData(value),schema,"samples","test",1,"samples",warehouse);
    env.execute("schema-to-iceberg");
    var table=IcebergTables.loader(warehouse).loadCatalog().loadTable(org.apache.iceberg.catalog.TableIdentifier.of("test","samples"));
    assertEquals("1",table.currentSnapshot().summary().get("total-records"));
    assertEquals(org.apache.iceberg.types.Type.TypeID.VARIANT,table.schema().findType("_json_remainder").typeId());
    try(var connection=java.sql.DriverManager.getConnection("jdbc:duckdb:");var statement=connection.createStatement()) {
      statement.execute("INSTALL iceberg; LOAD iceberg;");
      var metadata=((org.apache.iceberg.BaseTable)table).operations().current().metadataFileLocation();
      try(var result=statement.executeQuery("SELECT title, _json_remainder.extra.kept, details._json_remainder.unmapped FROM iceberg_scan('"+metadata.replace("'","''")+"')")) {
        assertTrue(result.next());assertEquals("sample",result.getString(1));
        assertTrue(result.getBoolean(2));assertEquals(42,result.getInt(3));
      }
    }
    // Read the committed Parquet with the same native Flink adapter, not a JSON mock.
    try(var tasks=table.newScan().planFiles()) {
      for(var task:tasks) {
        try(var records=org.apache.iceberg.parquet.Parquet.read(table.io().newInputFile(task.file().location()))
          .project(table.schema()).createReaderFunc(fileSchema->org.apache.iceberg.flink.data.FlinkParquetReaders.buildReader(table.schema(),fileSchema)).build()) {
          var read=(org.apache.flink.table.data.RowData)records.iterator().next();
          assertEquals("sample",read.getString(6).toString());
          assertTrue(read.getVariant(read.getArity()-1).getField("extra").getField("kept").getBoolean());
        }
      }
    }
  }

  @Test void flinkSqlProcessesVariantWithoutStringStorage() throws Exception {
    var mapping=new JsonSchemaRows("{\"type\":\"object\",\"properties\":{\"attributes\":{}}}");
    var env=org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.getExecutionEnvironment();env.setParallelism(1);
    var rows=env.fromData("{\"attributes\":{\"number\":7},\"unmapped\":[1,\"two\"]}")
      .map(mapping::read).returns(org.apache.flink.table.runtime.typeutils.ExternalTypeInfo.<Row>of(mapping.dataType()));
    var tables=org.apache.flink.table.api.bridge.java.StreamTableEnvironment.create(env);
    tables.createTemporaryView("input_records",tables.fromDataStream(rows));
    try(var result=tables.executeSql("SELECT CAST(JSON_VALUE(JSON_STRING(attributes), '$.number') AS BIGINT) AS n, _json_remainder FROM input_records").collect()) {
      var row=result.next();assertEquals(7L,row.getField(0));assertTrue(((Variant)row.getField(1)).isObject());
    }
  }

  @Test void rootArraysScalarsAndNullStayNativeVariant() {
    var mapping=new JsonSchemaRows("{}");
    for(String json:java.util.List.of("[1,\"two\",null]","true","null","42","\"text\"")) {
      assertEquals(Json.read(json),Json.read(((Variant)mapping.read(json).getField(0)).toJson()));
    }
  }
}
