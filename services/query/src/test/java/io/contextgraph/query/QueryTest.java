package io.contextgraph.query;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
class QueryTest {
 @TempDir Path dir;
 @Test @org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable(named="TEST_DUCKDB_EXTENSIONS", matches="true")
 void requiredExtensionsLoad() throws Exception {try(var c=IcebergQueries.connect(true); var s=c.createStatement();var r=s.executeQuery("SELECT count(*) FROM duckdb_extensions() WHERE extension_name IN ('iceberg','cache_httpfs') AND loaded")){assertTrue(r.next());assertEquals(2,r.getInt(1));}}
 @Test void historicalTimestampMatchesKafkaInstantIdentity() throws Exception {
  try(var c=IcebergQueries.connect(false)) {
   var rows=IcebergQueries.run(c,new IcebergQueries.BoundSql("SELECT TIMESTAMPTZ '2026-09-24 10:00:00-07' AS window_start",List.of()));
   assertEquals("2026-09-24T17:00:00Z",rows.getFirst().get("window_start"));
  }
 }
 @Test void configuredGraphqlSchemaIsValid() throws Exception {
  var config=QueryService.yaml(Path.of("../../config/queries.yaml"));
  StringBuilder sdl=new StringBuilder(config.path("types").asText()).append("\ntype Query {\n");
  config.path("queries").elements().forEachRemaining(q->sdl.append(q.path("signature").asText()).append('\n'));
  sdl.append("}\ntype Subscription {\n");config.path("subscriptions").elements().forEachRemaining(q->sdl.append(q.path("signature").asText()).append('\n'));sdl.append("}");
  var wiring=graphql.schema.idl.RuntimeWiring.newRuntimeWiring().scalar(graphql.scalars.ExtendedScalars.Json).scalar(graphql.scalars.ExtendedScalars.GraphQLLong).build();
  var schema=new graphql.schema.idl.SchemaGenerator().makeExecutableSchema(new graphql.schema.idl.SchemaParser().parse(sdl.toString()),wiring);
  assertNotNull(schema.getQueryType().getFieldDefinition("metrics"));assertNotNull(schema.getSubscriptionType().getFieldDefinition("videoChunk"));
 }
 @Test void routesOnlyMatchingTopicAndEntityIncludingBothEdgeEnds(){
  var metric=new LiveBus.Event("cg.metrics",Map.of("entity_id","sensor-a"));
  assertTrue(LiveBus.matches(metric,List.of("cg.metrics"),"entity_id","sensor-a"));
  assertFalse(LiveBus.matches(metric,List.of("cg.video"),"entity_id","sensor-a"));
  assertFalse(LiveBus.matches(metric,List.of("cg.metrics"),"entity_id","sensor-b"));
  var edge=new LiveBus.Event("cg.edges",Map.of("source_id","a","target_id","b"));
  assertTrue(LiveBus.matches(edge,List.of("cg.edges"),"node_id","b"));
  assertTrue(LiveBus.matches(new LiveBus.Event("cg.edges.v2", edge.payload()),List.of("cg.edges.v2"),"node_id","b"));
 }
 @Test void usesHintAndIgnoresUncommittedHigherVersion() throws Exception {
  Path md=Files.createDirectories(dir.resolve("context/metrics/metadata"));Files.writeString(md.resolve("version-hint.text"),"1");
  Files.writeString(md.resolve("v1.metadata.json"),"{\"current-schema-id\":0,\"schemas\":[{\"schema-id\":0,\"fields\":[{\"name\":\"entity_id\",\"type\":\"string\",\"required\":true}]}]}");
  Files.writeString(md.resolve("v2.metadata.json"),"uncommitted");
  var table=new IcebergQueries(dir).discover().get("context.metrics");assertTrue(table.metadata().endsWith("v1.metadata.json"));assertFalse(table.columns().getFirst().nullable());
 }
 @Test void bindsHostileUserValueWithoutChangingSqlAndClampsLimit() throws Exception {
  var bound=IcebergQueries.bind("SELECT :entityId AS entity_id LIMIT :limit",Map.of("entityId","x'; DROP TABLE source; --","limit",100000));
  assertEquals("SELECT ? AS entity_id LIMIT ?",bound.sql());assertEquals(10000,bound.values().get(1));
  try(var c=IcebergQueries.connect(false)){assertEquals("x'; DROP TABLE source; --",IcebergQueries.run(c,bound).getFirst().get("entity_id"));}
 }
}
