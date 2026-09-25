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
  public static final String[] INPUT_NAMES={"workspace_id","resource_id","entity_id","event_id","event_time","ingested_at","endpoint","kind","payload"};
  static final org.apache.flink.api.common.typeinfo.TypeInformation<Row> INPUT_TYPE=Types.ROW_NAMED(INPUT_NAMES,
      Types.STRING,Types.STRING,Types.STRING,Types.STRING,Types.LOCAL_DATE_TIME,Types.LOCAL_DATE_TIME,Types.STRING,Types.STRING,Types.STRING);
  public static Row input(String value) {
    var e=Scope.validate(Json.read(value));
    return Row.of(e.path("security").path("workspaceId").asText(),e.path("security").path("resourceId").asText(),e.path("entityId").asText(),
      e.path("eventId").asText(),LocalDateTime.ofInstant(Instant.parse(e.path("eventTime").asText()),ZoneOffset.UTC),
      LocalDateTime.ofInstant(Instant.parse(e.path("ingestedAt").asText()),ZoneOffset.UTC),e.path("endpoint").asText(),e.path("kind").asText(),e.path("payload").toString());
  }
  static Schema inputSchema() {return Schema.newBuilder().column("workspace_id",DataTypes.STRING()).column("resource_id",DataTypes.STRING())
      .column("entity_id",DataTypes.STRING()).column("event_id",DataTypes.STRING()).column("event_time",DataTypes.TIMESTAMP(3))
      .column("ingested_at",DataTypes.TIMESTAMP(3)).column("endpoint",DataTypes.STRING()).column("kind",DataTypes.STRING()).column("payload",DataTypes.STRING())
      .watermark("event_time","SOURCE_WATERMARK()").build();}
  public static void main(String[] args)throws Exception {
    boolean validate=args.length>0&&args[0].equals("--validate");
    Path path=Path.of(args[validate?1:0]); JsonNode bundle=Json.MAPPER.readTree(path.toFile());
    String workspace=bundle.path("workspace").asText(),name=bundle.path("name").asText();
    io.contextgraph.security.AccessControl.validateWorkspace(workspace);
    var settings=new Configuration();settings.setString("state.backend.type","rocksdb");
    settings.setString("execution.checkpointing.incremental","true");
    settings.setString("execution.checkpointing.dir",ContextGraphJob.env("CHECKPOINT_URI","file:///tmp/context-checkpoints/"+name));
    settings.setString("execution.checkpointing.savepoint-dir",ContextGraphJob.env("SAVEPOINT_URI","file:///tmp/context-savepoints/"+name));
    settings.setString("execution.checkpointing.externalized-checkpoint-retention","RETAIN_ON_CANCELLATION");
    var env=StreamExecutionEnvironment.getExecutionEnvironment(settings);env.setParallelism(bundle.path("parallelism").asInt(1));env.setMaxParallelism(128);
    env.enableCheckpointing(10000);env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
    var tables=StreamTableEnvironment.create(env);tables.getConfig().setLocalTimeZone(ZoneOffset.UTC);
    String bootstrap=ContextGraphJob.env("KAFKA_BOOTSTRAP_SERVERS","secure-kafka:9092");
    String envelope=Files.readString(path.getParent().resolve("schemas/envelope.json"));
    var errorStreams=new ArrayList<org.apache.flink.streaming.api.datastream.DataStream<String>>();
    for(var source:bundle.path("sources")) {
      String alias=source.path("name").asText();
      org.apache.flink.streaming.api.datastream.DataStream<String> raw;
      if(validate) raw=env.fromData("{\"placeholder\":true}");
      else {
        var kafka=KafkaSource.<String>builder().setBootstrapServers(bootstrap).setProperties(KafkaSecurity.properties()).setTopics(source.path("topic").asText())
          .setGroupId(name+"-"+alias).setStartingOffsets(OffsetsInitializer.earliest()).setValueOnlyDeserializer(new SimpleStringSchema()).setProperty("isolation.level","read_committed").build();
        raw=env.fromSource(kafka,new EventTime(envelope,3,10),alias).uid("source-"+alias);
      }
      var checked=raw.process(new Transforms.Validate(envelope)).uid("validate-"+alias);
      errorStreams.add(checked.getSideOutput(Transforms.ERRORS));
      var rows=checked.filter(v->Json.read(v).path("security").path("workspaceId").asText().equals(workspace)).map(SqlBundleJob::input).returns(INPUT_TYPE);
      tables.createTemporaryView(alias,tables.fromDataStream(rows,inputSchema()));
    }
    var description=Json.object();
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
        output.sinkTo(ContextGraphJob.kafka(bootstrap,view.path("topic").asText(),viewName)).uid("live-"+viewName);
      }
    }
    if(validate) {Files.writeString(Path.of(args[2]),description.toPrettyString());return;}
    var errors=errorStreams.getFirst();for(int i=1;i<errorStreams.size();i++)errors=errors.union(errorStreams.get(i));
    var errorSchema=new Json.Validator(Files.readString(path.getParent().resolve("schemas/error.json")));
    errors.map(v->{errorSchema.validate(v);return v;}).returns(Types.STRING).sinkTo(ContextGraphJob.kafka(bootstrap,bundle.path("errorTopic").asText(),"errors")).uid("errors");
    // All relational branches and both destinations enter one execution graph; no waiting INSERT loop.
    env.execute(name);
  }
}
