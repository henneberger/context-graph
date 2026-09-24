package io.contextgraph.processor;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;

public final class Transforms {
  public static final OutputTag<String> ERRORS = new OutputTag<String>("errors") {};
  public static final OutputTag<String> LATE = new OutputTag<String>("late") {};
  public static final class Validate extends ProcessFunction<String, String> {
    private final Json.Validator validator;
    public Validate(String schema) { validator = new Json.Validator(schema); }
    @Override public void processElement(String value, Context ctx, Collector<String> out) {
      try {
        JsonNode e = Scope.validate(validator.validate(value));
        Instant.parse(e.path("eventTime").asText());
        Instant.parse(e.path("ingestedAt").asText());
        out.collect(value);
      } catch (Exception ex) { ctx.output(ERRORS, Json.error(value, ex.getMessage())); }
    }
  }
  public static String event(String value) {
    var e = Json.read(value);
    return Scope.row(e).put("event_id", e.path("eventId").asText())
        .put("entity_id", e.path("entityId").asText()).put("endpoint", e.path("endpoint").asText())
        .put("kind", e.path("kind").asText()).put("event_time", e.path("eventTime").asText())
        .put("ingested_at", e.path("ingestedAt").asText()).put("payload", e.path("payload").toString()).toString();
  }
  public static String node(String value, JobConfig.Graph graph) {
    var e = Json.read(value);
    return Scope.row(e).put("node_id", e.path("entityId").asText())
        .put("node_type", e.at(graph.nodeTypePointer()).asText("entity"))
        .put("event_time", e.path("eventTime").asText()).put("source_event_id", e.path("eventId").asText())
        .put("properties", e.path("payload").toString()).toString();
  }
  public static String edge(String value, JobConfig.Graph graph) {
    var e = Json.read(value);
    String target = e.at(graph.targetPointer()).asText("");
    if (target.isBlank()) return null;
    var scoped = Scope.row(e);
    String targetResource = Scope.resource(scoped.path("workspace_id").asText(), target);
    String id = scoped.path("resource_id").asText() + "\u0000" + targetResource + "\u0000" + graph.relation();
    return scoped.put("target_resource_id", targetResource)
        .put("edge_id", UUID.nameUUIDFromBytes(id.getBytes(StandardCharsets.UTF_8)).toString())
        .put("source_id", e.path("entityId").asText()).put("target_id", target).put("relation", graph.relation())
        .put("event_time", e.path("eventTime").asText()).put("source_event_id", e.path("eventId").asText())
        .put("properties", "{}").toString();
  }
  public static final class Samples extends ProcessFunction<String, String> {
    private final JobConfig.Aggregation config;
    public Samples(JobConfig.Aggregation config) { this.config = config; }
    @Override public void processElement(String value, Context ctx, Collector<String> out) {
      var e = Json.read(value);
      JsonNode n = e.at(config.valuePointer());
      if (n.isMissingNode() || n.isNull()) return;
      double number = n.asDouble(Double.NaN) * config.scale() + config.offset();
      if (!n.isNumber() || !Double.isFinite(number)) {
        ctx.output(ERRORS, Json.error(value, "Non-finite/non-numeric aggregation field " + config.valuePointer()));
        return;
      }
      out.collect(Scope.row(e).put("entity_id", e.path("entityId").asText())
          .put("metric", e.at(config.metricPointer()).asText(config.defaultMetric()))
          .put("value", number).toString());
    }
  }
  public static String sampleKey(String s) {
    var e = Json.read(s);
    String workspace = e.path("workspace_id").asText();
    String entity = e.path("entity_id").asText();
    String resource = e.path("resource_id").asText();
    if (!resource.equals(Scope.resource(workspace, entity))) throw new IllegalArgumentException("Invalid sample scope");
    return Json.MAPPER.createArrayNode().add(workspace).add(resource).add(entity).add(e.path("metric").asText()).toString();
  }
  public static class Stats implements AggregateFunction<String, double[], double[]> {
    @Override public double[] createAccumulator() { return new double[]{0, 0, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY}; }
    @Override public double[] add(String value, double[] a) {
      double n = Json.read(value).path("value").asDouble();
      a[0]++; a[1] += n; a[2] = Math.min(a[2], n); a[3] = Math.max(a[3], n); return a;
    }
    @Override public double[] getResult(double[] a) { return a; }
    @Override public double[] merge(double[] a, double[] b) {
      return new double[]{a[0]+b[0],a[1]+b[1],Math.min(a[2],b[2]),Math.max(a[3],b[3])};
    }
  }
  public static class FinishWindow extends ProcessWindowFunction<double[], String, String, TimeWindow> {
    @Override public void process(String key, Context c, Iterable<double[]> input, Collector<String> out) {
      out.collect(metric(key, c.window().getStart(), c.window().getEnd(), input.iterator().next()));
    }
  }
  public static String metric(String key, long start, long end, double[] a) {
    var k = Json.read(key);
    return Json.object().put("workspace_id",k.get(0).asText()).put("resource_id",k.get(1).asText())
        .put("entity_id",k.get(2).asText()).put("metric",k.get(3).asText())
        .put("window_start", Instant.ofEpochMilli(start).toString()).put("window_end",Instant.ofEpochMilli(end).toString())
        .put("sample_count", (long)a[0]).put("sum_value",a[1]).put("avg_value",a[1]/a[0])
        .put("min_value",a[2]).put("max_value",a[3]).toString();
  }
}
