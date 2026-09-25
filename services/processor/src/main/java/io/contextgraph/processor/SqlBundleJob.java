package io.contextgraph.processor;

import com.fasterxml.jackson.databind.JsonNode;
import io.contextgraph.security.KafkaSecurity;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/** Runtime for compiler-produced bundles. Credentials and connectors are platform bindings. */
public final class SqlBundleJob {
  static org.apache.flink.table.types.DataType sourceType(JsonSchemaRows mapping) {
    var fields=new ArrayList<DataTypes.Field>();
    for(String name:List.of("workspace_id","resource_id","entity_id","event_id"))fields.add(DataTypes.FIELD(name,DataTypes.STRING()));
    fields.add(DataTypes.FIELD("event_time",DataTypes.TIMESTAMP(3)));fields.add(DataTypes.FIELD("ingested_at",DataTypes.TIMESTAMP(3)));
    var payload=(org.apache.flink.table.types.logical.RowType)mapping.dataType().getLogicalType();
    var reserved=Set.of("workspace_id","resource_id","entity_id","event_id","event_time","ingested_at");
    for(var f:payload.getFields()) {
      if(reserved.contains(f.getName()))throw new IllegalArgumentException("Input field collides with transport metadata: "+f.getName());
      fields.add(DataTypes.FIELD(f.getName(),org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType(f.getType())));
    }
    return DataTypes.ROW(fields.toArray(DataTypes.Field[]::new));
  }
  static Row input(String value,JsonSchemaRows mapping) {
    var e=Scope.validate(Json.read(value));var payload=mapping.read(e.path("payload").toString());
    Row row=new Row(payload.getArity()+6);
    row.setField(0,e.path("security").path("workspaceId").asText());row.setField(1,e.path("security").path("resourceId").asText());
    row.setField(2,e.path("entityId").asText());row.setField(3,e.path("eventId").asText());
    row.setField(4,LocalDateTime.ofInstant(Instant.parse(e.path("eventTime").asText()),ZoneOffset.UTC));
    row.setField(5,LocalDateTime.ofInstant(Instant.parse(e.path("ingestedAt").asText()),ZoneOffset.UTC));
    for(int i=0;i<payload.getArity();i++)row.setField(i+6,payload.getField(i));return row;
  }
  public static void main(String[] args)throws Exception {
    boolean validate=args.length>0&&args[0].equals("--validate");
    Path path=Path.of(args[validate?1:0]); JsonNode bundle=new com.fasterxml.jackson.databind.ObjectMapper(new com.fasterxml.jackson.dataformat.yaml.YAMLFactory()).readTree(path.toFile());
    String workspace=bundle.path("workspace").asText(),name=bundle.path("name").asText();
    io.contextgraph.security.AccessControl.validateWorkspace(workspace);
    var settings=new Configuration();settings.setString("state.backend.type","rocksdb");
    settings.setString("execution.checkpointing.incremental","true");
    settings.setString("execution.checkpointing.dir",RuntimeSupport.env("CHECKPOINT_URI","file:///tmp/context-checkpoints/"+name));
    settings.setString("execution.checkpointing.savepoint-dir",RuntimeSupport.env("SAVEPOINT_URI","file:///tmp/context-savepoints/"+name));
    settings.setString("execution.checkpointing.externalized-checkpoint-retention","RETAIN_ON_CANCELLATION");
    var env=StreamExecutionEnvironment.getExecutionEnvironment(settings);env.setParallelism(bundle.path("parallelism").asInt(1));env.setMaxParallelism(128);
    env.enableCheckpointing(10000);env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
    var tables=StreamTableEnvironment.create(env);tables.getConfig().setLocalTimeZone(ZoneOffset.UTC);
    String bootstrap=RuntimeSupport.env("KAFKA_BOOTSTRAP_SERVERS","secure-kafka:9092");
    String envelope=Files.readString(path.getParent().resolve("schemas/envelope.json"));
    var errorStreams=new ArrayList<org.apache.flink.streaming.api.datastream.DataStream<String>>();
    var description=Json.object();
    for(var source:bundle.path("sources")) {
      String alias=source.path("name").asText();
      String sourceWorkspace=source.path("workspace").asText(workspace);
      io.contextgraph.security.AccessControl.validateWorkspace(sourceWorkspace);
      var mapping=new JsonSchemaRows(Files.readString(path.getParent().resolve(source.path("schema").asText())));
      var type=sourceType(mapping);
      org.apache.flink.streaming.api.datastream.DataStream<String> raw;
      if(validate) raw=env.fromData("{\"placeholder\":true}");
      else {
        var kafka=KafkaSource.<String>builder().setBootstrapServers(bootstrap).setProperties(KafkaSecurity.properties()).setTopics(source.path("topic").asText())
          .setGroupId(name+"-"+alias).setStartingOffsets(OffsetsInitializer.earliest()).setValueOnlyDeserializer(new SimpleStringSchema()).setProperty("isolation.level","read_committed").build();
        raw=env.fromSource(kafka,new EventTime(envelope,3,10),alias).uid("source-"+alias);
      }
      var checked=raw.process(new Transforms.Validate(envelope)).uid("validate-"+alias);
      errorStreams.add(checked.getSideOutput(Transforms.ERRORS));
      var rows=checked.process(new org.apache.flink.streaming.api.functions.ProcessFunction<String,Row>() {
        public void processElement(String value,Context context,org.apache.flink.util.Collector<Row> out) {
          try {
            if(!Json.read(value).path("security").path("workspaceId").asText().equals(sourceWorkspace))throw new IllegalArgumentException("Workspace mismatch");
            out.collect(input(value,mapping));
          }catch(RuntimeException invalid){context.output(Transforms.ERRORS,Json.error(value,invalid.getMessage()));}
        }
      }).returns(org.apache.flink.table.runtime.typeutils.ExternalTypeInfo.<Row>of(type)).uid("schema-"+alias);
      errorStreams.add(rows.getSideOutput(Transforms.ERRORS));
      var inputSchema=Schema.newBuilder().fromRowDataType(type).watermark("event_time","SOURCE_WATERMARK()").build();
      tables.createTemporaryView(alias,tables.fromDataStream(rows,inputSchema));
      var icebergSchema=new org.apache.iceberg.Schema(org.apache.iceberg.flink.FlinkSchemaUtil.convert(type.getLogicalType()).asStructType().fields());
      description.putObject(alias).set("schema",Json.MAPPER.readTree(org.apache.iceberg.SchemaParser.toJson(icebergSchema)));
      if(!validate) {
        var output=rows.map(row->GenericIceberg.encode(row,icebergSchema,sourceWorkspace)).returns(Types.STRING).uid("encode-input-"+alias);
        GenericIceberg.attach(output,icebergSchema,source.path("table").asText(alias),bundle.path("namespace").asText(),env.getParallelism(),"input-"+alias);
        output.sinkTo(RuntimeSupport.kafka(bootstrap,source.path("changeTopic").asText(),alias)).uid("live-input-"+alias);
      }
    }
    for(var view:bundle.path("views")) {
      String viewName=view.path("name").asText(); Table result=tables.sqlQuery(view.path("sql").asText());
      tables.createTemporaryView(viewName,result);
      var schema=new org.apache.iceberg.Schema(org.apache.iceberg.flink.FlinkSchemaUtil.convert(result.getResolvedSchema().toPhysicalRowDataType().getLogicalType()).asStructType().fields());
      GenericIceberg.requireSupported(schema);
      var entry=description.putObject(viewName);entry.set("schema",Json.MAPPER.readTree(org.apache.iceberg.SchemaParser.toJson(schema)));
      // toDataStream rejects updating queries: append-only outputs are an explicit v1 contract.
      var rows=tables.toDataStream(result);
      entry.put("plan",result.explain());
      if(!validate) {
        var output=rows.map(row->GenericIceberg.encode(row,schema,workspace)).returns(Types.STRING).uid("encode-"+viewName);
        GenericIceberg.attach(output,schema,view.path("table").asText(),bundle.path("namespace").asText(),env.getParallelism(),viewName);
        output.sinkTo(RuntimeSupport.kafka(bootstrap,view.path("topic").asText(),viewName)).uid("live-"+viewName);
      }
    }
    if(validate) {Files.writeString(Path.of(args[2]),description.toPrettyString());return;}
    var errors=errorStreams.getFirst();for(int i=1;i<errorStreams.size();i++)errors=errors.union(errorStreams.get(i));
    var errorSchema=new Json.Validator(Files.readString(path.getParent().resolve("schemas/error.json")));
    errors.map(v->{errorSchema.validate(v);return v;}).returns(Types.STRING).sinkTo(RuntimeSupport.kafka(bootstrap,bundle.path("errorTopic").asText(),"errors")).uid("errors");
    // All relational branches and both destinations enter one execution graph; no waiting INSERT loop.
    env.execute(name);
  }
}
