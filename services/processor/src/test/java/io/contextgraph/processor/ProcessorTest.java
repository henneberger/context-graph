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
}
