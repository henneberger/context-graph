package io.contextgraph.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.Serializable;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

public record JobConfig(String name, int parallelism, List<String> sourceTopics, String consumerGroup,
    String envelopeSchema, String errorTopic, long checkpointIntervalMs, long checkpointMinPauseMs,
    long watermarkDelaySeconds, long idleSeconds, String namespace, Map<String, Output> outputs,
    Graph graph, List<Aggregation> aggregations) implements Serializable {
  public record Output(String table, String topic, String schema) implements Serializable {}
  public record Graph(String nodeTypePointer, String targetPointer, String relation) implements Serializable {}
  public record Aggregation(String id, String valuePointer, String metricPointer, String defaultMetric,
      int windowSeconds, double scale, double offset) implements Serializable {}
  public static JobConfig load(Path path) throws Exception {
    var c = new ObjectMapper(new YAMLFactory()).readValue(path.toFile(), JobConfig.class);
    if (c.parallelism < 1 || c.sourceTopics == null || c.sourceTopics.isEmpty()
        || c.checkpointIntervalMs < 1000 || c.checkpointMinPauseMs < 0 || c.idleSeconds < 1
        || c.watermarkDelaySeconds < 0 || c.name == null || c.consumerGroup == null)
      throw new IllegalArgumentException("Invalid job/checkpoint/source configuration");
    if (c.outputs == null || !c.outputs.keySet().containsAll(List.of("events", "nodes", "edges", "metrics")))
      throw new IllegalArgumentException("Configure events, nodes, edges and metrics outputs");
    for (var output : c.outputs.values()) {
      if (output.topic != null && (output.schema == null || output.schema.isBlank()))
        throw new IllegalArgumentException("Every Kafka output requires an explicit JSON Schema");
    }
    var ids = new java.util.HashSet<String>();
    for (var a : c.aggregations) {
      if (a.windowSeconds < 1 || !ids.add(a.id) || !a.valuePointer.startsWith("/")
          || !Double.isFinite(a.scale) || !Double.isFinite(a.offset))
        throw new IllegalArgumentException("Invalid/duplicate aggregation " + a.id);
    }
    return c;
  }
}
