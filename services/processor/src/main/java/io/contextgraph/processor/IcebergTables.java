package io.contextgraph.processor;

import java.time.Instant;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.types.Types;

public final class IcebergTables {
  public static Schema schema(String kind) {
    return switch (kind) {
      case "events" -> fields("event_id:s,entity_id:s,endpoint:s,kind:s,event_time:t,ingested_at:t,payload:s");
      case "nodes" -> fields("node_id:s,node_type:s,event_time:t,source_event_id:s,properties:s");
      case "edges" -> fields("edge_id:s,source_id:s,target_id:s,relation:s,event_time:t,source_event_id:s,properties:s");
      case "metrics" -> fields("entity_id:s,metric:s,window_start:t,window_end:t,sample_count:l,sum_value:d,avg_value:d,min_value:d,max_value:d");
      default -> throw new IllegalArgumentException("Unknown table kind " + kind);
    };
  }
  private static Schema fields(String definition) {
    var columns = new java.util.ArrayList<Types.NestedField>();
    int id = 1;
    for (String field : (definition + ",workspace_id:s,resource_id:s" + (definition.startsWith("edge_id:") ? ",target_resource_id:s" : "")).split(",")) {
      String[] p = field.split(":");
      var type = switch (p[1]) {
        case "t" -> Types.TimestampType.withZone();
        case "l" -> Types.LongType.get();
        case "d" -> Types.DoubleType.get();
        default -> Types.StringType.get();
      };
      columns.add(Types.NestedField.required(id++, p[0], type));
    }
    return new Schema(columns);
  }
  public static RowData row(String json, String kind) {
    var node = Json.read(json);
    Schema s = schema(kind);
    GenericRowData row = new GenericRowData(s.columns().size());
    for (int i = 0; i < s.columns().size(); i++) {
      var field = s.columns().get(i);
      var value = node.get(field.name());
      if (value == null || value.isNull()) throw new IllegalArgumentException("Missing " + field.name());
      Object data = switch (field.type().typeId()) {
        case TIMESTAMP -> TimestampData.fromInstant(Instant.parse(value.asText()));
        case LONG -> value.asLong();
        case DOUBLE -> value.asDouble();
        default -> StringData.fromString(value.asText());
      };
      row.setField(i, data);
    }
    return row;
  }
  public static CatalogLoader loader(String warehouse) {
    String uri=System.getenv("POLARIS_URI");
    if(uri!=null) {
      if(!uri.startsWith("https://"))throw new IllegalArgumentException("Polaris requires TLS");
      return CatalogLoader.rest("context",new org.apache.hadoop.conf.Configuration(),Map.of(
        "uri",uri,"warehouse",System.getenv().getOrDefault("POLARIS_WAREHOUSE","context"),
        "credential",java.util.Objects.requireNonNull(System.getenv("POLARIS_CREDENTIAL")),
        "oauth2-server-uri",uri+"/v1/oauth/tokens","scope","PRINCIPAL_ROLE:ALL",
        "header.X-Iceberg-Access-Delegation","vended-credentials","io-impl","org.apache.iceberg.aws.s3.S3FileIO"));
    }
    return CatalogLoader.hadoop("context", new org.apache.hadoop.conf.Configuration(),
        Map.of("warehouse", warehouse));
  }
  public static void attach(DataStream<String> stream, String kind, String tableName,
      String namespace, String warehouse, int parallelism) {
    var catalogLoader = loader(warehouse);
    var catalog = catalogLoader.loadCatalog();
    TableIdentifier id = TableIdentifier.of(namespace, tableName);
    Schema expected = schema(kind);
    Table table;
    if (catalog.tableExists(id)) {
      table = catalog.loadTable(id);
      if (!table.schema().sameSchema(expected))
        throw new IllegalStateException("Incompatible existing schema for " + id + "; migrate or version the output table");
    } else {
      String timestamp = kind.equals("metrics") ? "window_start" : "event_time";
      table = catalog.createTable(id, expected, PartitionSpec.builderFor(expected).day(timestamp).build(),
          Map.of("format-version", "2", "write.format.default", "parquet", "write.target-file-size-bytes", "134217728"));
    }
    var rows = stream.map(value -> row(value, kind)).returns(org.apache.flink.table.runtime.typeutils.InternalTypeInfo.of(org.apache.iceberg.flink.FlinkSchemaUtil.convert(expected)))
        .name(kind + " typed Iceberg rows").uid("iceberg-rows-" + kind);
    FlinkSink.forRowData(rows).table(table).tableLoader(TableLoader.fromCatalog(catalogLoader, id))
        .writeParallelism(parallelism).uidPrefix("iceberg-" + kind).append();
  }
}
