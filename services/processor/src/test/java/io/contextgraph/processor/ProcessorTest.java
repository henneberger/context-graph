package io.contextgraph.processor;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import static org.junit.jupiter.api.Assertions.*;

class ProcessorTest {
  @TempDir Path temp;
  static final String EVENT = scoped("workspace-a", """
      {"eventId":"af560aa1-fc08-4b71-b233-c02062537bc8","schemaVersion":2,"endpoint":"events","kind":"json","entityId":"sensor-1","eventTime":"2026-09-24T18:00:01Z","ingestedAt":"2026-09-24T18:00:02Z","payload":{"value":4,"metric":"temperature","relatedTo":"room-1","arbitrary":{"retained":true}}}
      """);
  static String scoped(String workspace, String raw) {
    var event = (com.fasterxml.jackson.databind.node.ObjectNode) Json.read(raw);
    event.put("schemaVersion", 2);
    event.set("security", Json.object().put("workspaceId", workspace)
        .put("resourceId", Scope.resource(workspace,event.path("entityId").asText())).put("subjectId", "alice"));
    return event.toString();
  }
  @Test void validatesAndPreservesUnstructuredPayload() throws Exception {
    var validator = new Json.Validator(Files.readString(Path.of("../../config/schemas/envelope.json")));
    assertDoesNotThrow(() -> validator.validate(EVENT));
    assertThrows(IllegalArgumentException.class, () -> validator.validate("{\"kind\":\"json\"}"));
    assertTrue(Json.read(Transforms.event(EVENT)).path("payload").asText().contains("retained"));
    var graph = new JobConfig.Graph("/kind", "/payload/relatedTo", "related_to");
    assertEquals("room-1",Json.read(Transforms.edge(EVENT,graph)).path("target_id").asText());
    assertEquals(Transforms.edge(EVENT,graph),Transforms.edge(EVENT,graph));
    assertNull(Transforms.edge(EVENT, new JobConfig.Graph("/kind","/payload/missing","related_to")));
  }
  @Test void eventTimeWindowExecutesOnFlink23() throws Exception {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var aggregation = new JobConfig.Aggregation("test","/payload/value","/payload/metric","value",10,2,1);
    var values = env.fromData(EVENT, EVENT.replace("\"value\":4", "\"value\":6"))
        .assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ZERO)
            .withTimestampAssigner((s, previous) -> Instant.parse(Json.read(s).path("eventTime").asText()).toEpochMilli()))
        .process(new Transforms.Samples(aggregation)).keyBy(Transforms::sampleKey)
        .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
        .aggregate(new Transforms.Stats(),new Transforms.FinishWindow());
    var results = new ArrayList<String>();
    try (var iterator = values.executeAndCollect()) { iterator.forEachRemaining(results::add); }
    assertEquals(1,results.size());
    var result = Json.read(results.getFirst());
    assertEquals(2,result.path("sample_count").asLong());
    assertEquals(22,result.path("sum_value").asDouble());
    assertEquals(11,result.path("avg_value").asDouble());
    assertEquals("2026-09-24T18:00:00Z",result.path("window_start").asText());
  }
  @Test void rejectsMissingAndForgedScopeBeforeWatermarks() throws Exception {
    var event = (com.fasterxml.jackson.databind.node.ObjectNode) Json.read(EVENT);
    event.remove("security");
    event.put("schemaVersion",1);
    assertThrows(IllegalArgumentException.class, () -> Scope.validate(event));
    var forged = (com.fasterxml.jackson.databind.node.ObjectNode) Json.read(EVENT);
    ((com.fasterxml.jackson.databind.node.ObjectNode) forged.path("security")).put("workspaceId","workspace-b");
    assertThrows(IllegalArgumentException.class, () -> Scope.validate(forged));
    var wm = new EventTime(Files.readString(Path.of("../../config/schemas/envelope.json")),3,10);
    assertEquals(Long.MIN_VALUE,wm.createTimestampAssigner(null).extractTimestamp(forged.toString(),0));
    assertEquals(Long.MIN_VALUE,wm.createTimestampAssigner(null).extractTimestamp(event.toString(),0));
  }
  @Test void sameEntityInDifferentWorkspacesNeverAggregatesTogether() throws Exception {
    var env = StreamExecutionEnvironment.getExecutionEnvironment(); env.setParallelism(2);
    var a = new JobConfig.Aggregation("secure","/payload/value","/payload/metric","value",10,1,0);
    var values = env.fromData(EVENT,scoped("workspace-b",EVENT).replace("\"value\":4","\"value\":40"))
        .assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ZERO)
            .withTimestampAssigner((s,old) -> Instant.parse(Json.read(s).path("eventTime").asText()).toEpochMilli()))
        .process(new Transforms.Samples(a)).keyBy(Transforms::sampleKey)
        .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10))).aggregate(new Transforms.Stats(),new Transforms.FinishWindow());
    var results = new ArrayList<String>();
    try (var it=values.executeAndCollect()) { it.forEachRemaining(results::add); }
    assertEquals(2,results.size());
    for (String raw : results) {
      var metric=Json.read(raw); String workspace=metric.path("workspace_id").asText();
      assertEquals(1,metric.path("sample_count").asInt());
      assertEquals(workspace.equals("workspace-a") ? 4 : 40,metric.path("sum_value").asDouble());
      assertEquals(Scope.resource(workspace,"sensor-1"),metric.path("resource_id").asText());
    }
  }
  @Test void graphLabelsAndKafkaKeysAreCanonicalAndScoped() {
    assertEquals("547b661f8128c8c5daec8c51536b2d76807c94ed0cee582d25244410300633fb",Scope.resource("workspace-a","sensor-1"));
    var graph = new JobConfig.Graph("/kind","/payload/relatedTo","related_to");
    var first=Json.read(Transforms.edge(EVENT,graph));
    var second=Json.read(Transforms.edge(scoped("workspace-b",EVENT),graph));
    assertEquals(Scope.resource("workspace-a","sensor-1"),first.path("resource_id").asText());
    assertEquals(Scope.resource("workspace-a","room-1"),first.path("target_resource_id").asText());
    assertNotEquals(first.path("edge_id"),second.path("edge_id"));
    assertNotEquals(ContextGraphJob.kafkaKey(first.toString(),"edges"),ContextGraphJob.kafkaKey(second.toString(),"edges"));
    ((com.fasterxml.jackson.databind.node.ObjectNode) first).put("target_resource_id",Scope.resource("workspace-b","room-1"));
    assertThrows(IllegalArgumentException.class,()->ContextGraphJob.kafkaKey(first.toString(),"edges"));
    for(String kind : java.util.List.of("events","nodes","metrics","edges")) {
      assertNotNull(IcebergTables.schema(kind).findField("workspace_id"));
      assertNotNull(IcebergTables.schema(kind).findField("resource_id"));
    }
  }
  @Test void invalidProvenanceGoesOnlyToDeadLetterStream() throws Exception {
    var legacy=(com.fasterxml.jackson.databind.node.ObjectNode)Json.read(EVENT);
    legacy.remove("security"); legacy.put("schemaVersion",1);
    var forged=(com.fasterxml.jackson.databind.node.ObjectNode)Json.read(EVENT);
    ((com.fasterxml.jackson.databind.node.ObjectNode)forged.path("security")).put("resourceId","0".repeat(64));
    var env=StreamExecutionEnvironment.getExecutionEnvironment(); env.setParallelism(1);
    var valid=env.fromData(EVENT,legacy.toString(),forged.toString()).process(new Transforms.Validate(Files.readString(Path.of("../../config/schemas/envelope.json"))));
    var result=valid.map(v->"accepted").returns(Types.STRING).union(valid.getSideOutput(Transforms.ERRORS).map(v->"rejected").returns(Types.STRING));
    var rows=new ArrayList<String>();
    try(var it=result.executeAndCollect()) { it.forEachRemaining(rows::add); }
    assertEquals(1,rows.stream().filter("accepted"::equals).count());
    assertEquals(2,rows.stream().filter("rejected"::equals).count());
  }
  @Test void iceberg21RuntimeCommitsOnFlink23() throws Exception {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    String warehouse = temp.resolve("warehouse").toUri().toString();
    var events = env.fromData(EVENT).map(Transforms::event).returns(Types.STRING);
    IcebergTables.attach(events,"events","events","context",warehouse,1);
    env.execute("iceberg compatibility");
    var table = IcebergTables.loader(warehouse).loadCatalog().loadTable(org.apache.iceberg.catalog.TableIdentifier.of("context","events"));
    assertNotNull(table.currentSnapshot());
    assertEquals("1", table.currentSnapshot().summary().get("total-records"));
  }
  @org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable(named="KAFKA_CLIENT_CONFIG",matches=".+")
  @Test void completeJobBuildsWithKafkaConnector22() throws Exception {
    var config = JobConfig.load(Path.of("../../config/jobs.yaml"));
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    ContextGraphJob.build(env.fromData(EVENT),config,Path.of("../../config"),"localhost:9092",temp.resolve("full").toUri().toString());
    assertTrue(env.getExecutionPlan().contains("window-value-10s") || env.getExecutionPlan().contains("Window"));
  }
}
