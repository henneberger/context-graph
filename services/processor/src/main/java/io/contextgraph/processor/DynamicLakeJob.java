package io.contextgraph.processor;

import io.contextgraph.security.KafkaSecurity;
import java.nio.file.Path;
import java.util.regex.Pattern;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.flink.sink.dynamic.DynamicIcebergSink;

/** One DataStream source and one dynamic sink for an entire Kafka-backed lake. */
public final class DynamicLakeJob {
  public static void main(String[] args) throws Exception {
    if (args.length != 1) throw new IllegalArgumentException("Usage: DynamicLakeJob <lake.yaml>");
    var config = new com.fasterxml.jackson.databind.ObjectMapper(new com.fasterxml.jackson.dataformat.yaml.YAMLFactory())
        .readTree(Path.of(args[0]).toFile());
    String name = required(config, "name"), namespace = required(config, "namespace");
    if (!namespace.matches("[a-z][a-z0-9_]*")) throw new IllegalArgumentException("Invalid lake namespace");
    var topics = Pattern.compile(required(config, "topicPattern"));
    String cdcTopicPattern = config.path("cdcTopicPattern").asText("(?!)");
    Pattern.compile(cdcTopicPattern);
    int parallelism = config.path("parallelism").asInt(2);
    long checkpointMs = config.path("checkpointIntervalMs").asLong(30000);
    long discoveryMs = config.path("topicDiscoveryIntervalMs").asLong(30000);
    if (parallelism < 1 || checkpointMs < 1 || discoveryMs < 1)
      throw new IllegalArgumentException("Parallelism and intervals must be positive");
    var settings = new Configuration();
    settings.setString("execution.checkpointing.dir", RuntimeSupport.env("CHECKPOINT_URI", "file:///tmp/lake-checkpoints/" + name));
    settings.setString("execution.checkpointing.externalized-checkpoint-retention", "RETAIN_ON_CANCELLATION");
    var env = StreamExecutionEnvironment.getExecutionEnvironment(settings);
    env.setParallelism(parallelism); env.setMaxParallelism(128); env.enableCheckpointing(checkpointMs);
    env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
    var source = KafkaSource.<LakeRecord>builder()
        .setBootstrapServers(RuntimeSupport.env("KAFKA_BOOTSTRAP_SERVERS", "secure-kafka:9092"))
        .setProperties(KafkaSecurity.properties()).setTopicPattern(topics).setGroupId(name)
        .setStartingOffsets(OffsetsInitializer.earliest())
        .setProperty("partition.discovery.interval.ms", Long.toString(discoveryMs))
        .setProperty("isolation.level", "read_committed")
        .setDeserializer(new LakeRecord.Deserializer()).build();
    var stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "lake-kafka").uid("lake-kafka");
    var loader = IcebergTables.loader(RuntimeSupport.env("WAREHOUSE_URI", "file:///tmp/context-lake"));
    var catalog = loader.loadCatalog();
    try {
      if (catalog instanceof SupportsNamespaces namespaces) {
        try { namespaces.createNamespace(Namespace.of(namespace)); }
        catch (AlreadyExistsException exists) { /* Concurrent jobs may share a namespace. */ }
      }
    } finally { if (catalog instanceof AutoCloseable closeable) closeable.close(); }
    DynamicIcebergSink.forInput(stream).generator(new LakeRecordGenerator(namespace, parallelism, cdcTopicPattern))
        .catalogLoader(loader).writeParallelism(parallelism).uidPrefix("lake-iceberg")
        .immediateTableUpdate(true).dropUnusedColumns(false).append();
    env.execute(name);
  }

  private static String required(com.fasterxml.jackson.databind.JsonNode config, String field) {
    String value = config.path(field).asText();
    if (value.isBlank()) throw new IllegalArgumentException("Missing lake setting: " + field);
    return value;
  }
}
