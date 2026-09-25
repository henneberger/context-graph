package io.contextgraph.processor;

import java.time.*;
import java.util.*;
import org.apache.flink.table.data.*;
import org.apache.flink.types.Row;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.*;
import org.apache.iceberg.flink.sink.FlinkSink;

/** SQL-result schema binding shared by the bundle validator and streaming writer. */
public final class GenericIceberg {
  public static void requireSupported(Schema schema) {
    for(String column:List.of("workspace_id","resource_id","entity_id"))
      if(schema.findField(column)==null||schema.findType(column).typeId()!=org.apache.iceberg.types.Type.TypeID.STRING)throw new IllegalArgumentException("Output must preserve "+column);
    for(var f:schema.columns()) {
      if(!f.name().matches("[a-z][a-z0-9_]*"))throw new IllegalArgumentException("Output requires simple unique lowercase aliases");
      if(!Set.of(org.apache.iceberg.types.Type.TypeID.STRING,org.apache.iceberg.types.Type.TypeID.INTEGER,org.apache.iceberg.types.Type.TypeID.LONG,
        org.apache.iceberg.types.Type.TypeID.FLOAT,org.apache.iceberg.types.Type.TypeID.DOUBLE,org.apache.iceberg.types.Type.TypeID.BOOLEAN,
        org.apache.iceberg.types.Type.TypeID.TIMESTAMP).contains(f.type().typeId()))throw new IllegalArgumentException("Explicitly cast unsupported output type: "+f.name());
    }
  }
  public static String encode(Row row,Schema schema,String workspace) {
    if(row.getKind()!=org.apache.flink.types.RowKind.INSERT)throw new IllegalArgumentException("Append-only output required");
    var node=Json.object();
    for(int i=0;i<schema.columns().size();i++) {
      Object value=row.getField(i);String name=schema.columns().get(i).name();
      if(value instanceof LocalDateTime t)value=t.toInstant(ZoneOffset.UTC).toString();
      else if(value instanceof Instant t)value=t.toString();
      else if(value instanceof java.sql.Timestamp t)value=t.toInstant().toString();
      node.set(name,Json.MAPPER.valueToTree(value));
    }
    if(!workspace.equals(node.path("workspace_id").asText())||!Scope.resource(workspace,node.path("entity_id").asText()).equals(node.path("resource_id").asText()))
      throw new IllegalArgumentException("Invalid SQL output scope");
    // Validate against the derived schema, including nullability and primitive value kinds.
    for(var f:schema.columns()) {
      var v=node.get(f.name());if(v.isNull()){if(f.isRequired())throw new IllegalArgumentException("Required output: "+f.name());continue;}
      boolean valid=switch(f.type().typeId()) {
        case STRING,TIMESTAMP->v.isTextual();case BOOLEAN->v.isBoolean();case INTEGER,LONG->v.isIntegralNumber();
        case FLOAT,DOUBLE->v.isNumber()&&Double.isFinite(v.asDouble());default->false;
      };if(!valid)throw new IllegalArgumentException("Invalid output: "+f.name());
    }
    return node.toString();
  }
  static RowData row(String value,Schema schema) {
    var n=Json.read(value);var result=new GenericRowData(schema.columns().size());
    for(int i=0;i<schema.columns().size();i++) {var f=schema.columns().get(i);var v=n.get(f.name());if(v==null||v.isNull()){result.setField(i,null);continue;}
      result.setField(i,switch(f.type().typeId()) {case STRING->StringData.fromString(v.asText());case TIMESTAMP->TimestampData.fromInstant(Instant.parse(v.asText()));
        case INTEGER->v.asInt();case LONG->v.asLong();case FLOAT->(float)v.asDouble();case DOUBLE->v.asDouble();case BOOLEAN->v.asBoolean();default->throw new IllegalArgumentException();});
    }return result;
  }
  static void attach(org.apache.flink.streaming.api.datastream.DataStream<String> values,Schema schema,String tableName,String namespace,int parallelism,String uid) {
    var loader=IcebergTables.loader(ContextGraphJob.env("WAREHOUSE_URI","file:///tmp/context-warehouse"));var catalog=loader.loadCatalog();var id=TableIdentifier.of(namespace,tableName);
    org.apache.iceberg.Table table;
    if(catalog.tableExists(id)){table=catalog.loadTable(id);if(!table.schema().sameSchema(org.apache.iceberg.types.TypeUtil.reassignIds(schema,table.schema())))throw new IllegalArgumentException("Version incompatible output table: "+id);}
    else table=catalog.createTable(id,schema,PartitionSpec.builderFor(schema).identity("workspace_id").build(),Map.of("format-version","2","write.target-file-size-bytes","134217728"));
    var rows=values.map(v->row(v,schema)).returns(org.apache.flink.table.runtime.typeutils.InternalTypeInfo.of(FlinkSchemaUtil.convert(schema)));
    FlinkSink.forRowData(rows).table(table).tableLoader(TableLoader.fromCatalog(loader,id)).writeParallelism(parallelism).uidPrefix("iceberg-"+uid).append();
  }
}
