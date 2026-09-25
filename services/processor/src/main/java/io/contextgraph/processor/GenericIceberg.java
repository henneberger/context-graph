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
    for(var f:schema.columns()) requireType(f.type());
  }
  private static void requireType(org.apache.iceberg.types.Type type) {
    switch(type.typeId()) {
      case STRUCT -> type.asStructType().fields().forEach(f->requireType(f.type()));
      case LIST -> requireType(type.asListType().elementType());
      case MAP -> {requireType(type.asMapType().keyType());requireType(type.asMapType().valueType());}
      case STRING, INTEGER, LONG, FLOAT, DOUBLE, BOOLEAN, TIMESTAMP, DECIMAL, VARIANT -> {}
      default -> throw new IllegalArgumentException("Unsupported output type: "+type);
    }
  }
  public static String encode(Row row,Schema schema,String workspace) {
    if(row.getKind()!=org.apache.flink.types.RowKind.INSERT)throw new IllegalArgumentException("Append-only output required");
    var node=object(row,schema.asStruct());
    if(!workspace.equals(node.path("workspace_id").asText())||!Scope.resource(workspace,node.path("entity_id").asText()).equals(node.path("resource_id").asText()))
      throw new IllegalArgumentException("Invalid SQL output scope");
    return node.toString();
  }
  private static com.fasterxml.jackson.databind.node.ObjectNode object(Row row,org.apache.iceberg.types.Types.StructType type) {
    if(row.getArity()!=type.fields().size())throw new IllegalArgumentException("Row/schema arity mismatch");
    var out=Json.object();
    for(int i=0;i<row.getArity();i++){var f=type.fields().get(i);Object value=row.getField(i);
      if(value==null&&f.isRequired())throw new IllegalArgumentException("Required output: "+f.name());
      out.set(f.name(),json(value,f.type()));}
    return out;
  }
  private static com.fasterxml.jackson.databind.JsonNode json(Object value,org.apache.iceberg.types.Type type) {
    if(value==null)return com.fasterxml.jackson.databind.node.NullNode.instance;
    return switch(type.typeId()) {
      case VARIANT -> Json.read(((org.apache.flink.types.variant.Variant)value).toJson());
      case STRUCT -> object((Row)value,type.asStructType());
      case LIST -> {var out=Json.MAPPER.createArrayNode();for(Object item:(Object[])value)out.add(json(item,type.asListType().elementType()));yield out;}
      case MAP -> {var out=Json.object();((Map<?,?>)value).forEach((k,v)->out.set(k.toString(),json(v,type.asMapType().valueType())));yield out;}
      case TIMESTAMP -> Json.MAPPER.valueToTree(value instanceof LocalDateTime t?t.toInstant(ZoneOffset.UTC).toString():value.toString());
      case STRING -> {if(!(value instanceof String))throw new IllegalArgumentException("String required");yield Json.MAPPER.valueToTree(value);}
      case BOOLEAN -> {if(!(value instanceof Boolean))throw new IllegalArgumentException("Boolean required");yield Json.MAPPER.valueToTree(value);}
      case INTEGER,LONG -> {if(!(value instanceof Byte||value instanceof Short||value instanceof Integer||value instanceof Long))throw new IllegalArgumentException("Integer required");yield Json.MAPPER.valueToTree(value);}
      case FLOAT,DOUBLE,DECIMAL -> {if(!(value instanceof Number n)||!Double.isFinite(n.doubleValue()))throw new IllegalArgumentException("Finite number required");yield Json.MAPPER.valueToTree(value);}
      default -> throw new IllegalArgumentException("Unsupported output type: "+type);
    };
  }
  static RowData row(String value,Schema schema) {
    return (RowData)internal(Json.read(value),schema.asStruct());
  }
  private static Object internal(com.fasterxml.jackson.databind.JsonNode value,org.apache.iceberg.types.Type type) {
    if(value==null)return null;
    if(type.typeId()==org.apache.iceberg.types.Type.TypeID.VARIANT)return JsonSchemaRows.variant(value);
    if(value.isNull())return null;
    return switch(type.typeId()) {
      case STRING -> StringData.fromString(value.asText());
      case TIMESTAMP -> TimestampData.fromInstant(Instant.parse(value.asText()));
      case INTEGER -> value.intValue();case LONG -> value.longValue();case FLOAT -> value.floatValue();case DOUBLE -> value.doubleValue();case BOOLEAN -> value.booleanValue();
      case DECIMAL -> {var t=(org.apache.iceberg.types.Types.DecimalType)type;var decimal=DecimalData.fromBigDecimal(value.decimalValue(),t.precision(),t.scale());if(decimal==null)throw new IllegalArgumentException("Decimal overflow");yield decimal;}
      case VARIANT -> JsonSchemaRows.variant(value);
      case STRUCT -> {var fields=type.asStructType().fields();var row=new GenericRowData(fields.size());for(int i=0;i<fields.size();i++)row.setField(i,internal(value.get(fields.get(i).name()),fields.get(i).type()));yield row;}
      case LIST -> {Object[] items=new Object[value.size()];for(int i=0;i<items.length;i++)items[i]=internal(value.get(i),type.asListType().elementType());yield new GenericArrayData(items);}
      case MAP -> {Map<Object,Object> entries=new LinkedHashMap<>();value.fields().forEachRemaining(e->entries.put(StringData.fromString(e.getKey()),internal(e.getValue(),type.asMapType().valueType())));yield new GenericMapData(entries);}
      default -> throw new IllegalArgumentException("Unsupported output type: "+type);
    };
  }

  static void attach(org.apache.flink.streaming.api.datastream.DataStream<String> values,Schema schema,String tableName,String namespace,int parallelism,String uid) {
    attach(values,schema,tableName,namespace,parallelism,uid,RuntimeSupport.env("WAREHOUSE_URI","file:///tmp/context-warehouse"));
  }
  static void attach(org.apache.flink.streaming.api.datastream.DataStream<String> values,Schema schema,String tableName,String namespace,int parallelism,String uid,String warehouse) {
    var loader=IcebergTables.loader(warehouse);var catalog=loader.loadCatalog();var id=TableIdentifier.of(namespace,tableName);
    org.apache.iceberg.Table table;
    if(catalog.tableExists(id)){table=catalog.loadTable(id);if(!table.schema().sameSchema(org.apache.iceberg.types.TypeUtil.reassignIds(schema,table.schema())))throw new IllegalArgumentException("Version incompatible output table: "+id);}
    else table=catalog.createTable(id,schema,PartitionSpec.builderFor(schema).identity("workspace_id").build(),Map.of("format-version","3","write.target-file-size-bytes","134217728"));
    var rows=values.map(v->row(v,schema)).returns(org.apache.flink.table.runtime.typeutils.InternalTypeInfo.of(FlinkSchemaUtil.convert(schema)));
    FlinkSink.forRowData(rows).table(table).tableLoader(TableLoader.fromCatalog(loader,id)).writeParallelism(parallelism).uidPrefix("iceberg-"+uid).append();
  }
}
