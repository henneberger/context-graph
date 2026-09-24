package io.contextgraph.processor;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;
import io.contextgraph.security.KafkaSecurity;

public final class ContextGraphJob {
  public static String env(String name, String fallback) { return System.getenv().getOrDefault(name, fallback); }
  public static void main(String[] args) throws Exception {
    Path path = Path.of(args.length > 0 ? args[0] : env("CONFIG_PATH", "/app/config/jobs.yaml"));
    JobConfig config = JobConfig.load(path);
    Configuration settings = new Configuration();
    settings.setString("execution.checkpointing.savepoint-dir", env("SAVEPOINT_URI", "file:///data/savepoints"));
    settings.setString("state.backend.type", "rocksdb");
    settings.set(org.apache.flink.configuration.CheckpointingOptions.INCREMENTAL_CHECKPOINTS, true);
    settings.setString("execution.checkpointing.storage", "filesystem");
    settings.setString("execution.checkpointing.dir", env("CHECKPOINT_URI", "file:///data/checkpoints"));
    settings.setString("execution.checkpointing.externalized-checkpoint-retention", "RETAIN_ON_CANCELLATION");
    var execution = StreamExecutionEnvironment.getExecutionEnvironment(settings);
    execution.setParallelism(config.parallelism());
    execution.setMaxParallelism(128);
    execution.enableCheckpointing(config.checkpointIntervalMs(), CheckpointingMode.EXACTLY_ONCE);
    execution.getCheckpointConfig().setMinPauseBetweenCheckpoints(config.checkpointMinPauseMs());
    execution.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
    execution.getCheckpointConfig().setCheckpointTimeout(120_000);
    execution.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
    // Incremental RocksDB snapshots limit upload pressure. Aligned checkpoints avoid
    // adding in-flight network buffers to the persistent checkpoint under backpressure.
    String bootstrap = env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
    var source = KafkaSource.<String>builder().setBootstrapServers(bootstrap)
        .setProperties(KafkaSecurity.properties())
        .setTopics(config.sourceTopics()).setGroupId(config.consumerGroup())
        .setStartingOffsets(OffsetsInitializer.earliest()).setValueOnlyDeserializer(new SimpleStringSchema())
        .setProperty("isolation.level", "read_committed").build();
    var watermarks = new EventTime(Files.readString(path.toAbsolutePath().getParent().resolve(config.envelopeSchema())),
        config.watermarkDelaySeconds(), config.idleSeconds());
    var raw = execution.fromSource(source, watermarks, "endpoint topics").uid("endpoint-source");
    build(raw, config, path.toAbsolutePath().getParent(), bootstrap, env("WAREHOUSE_URI", "file:///data/warehouse"));
    execution.execute(config.name());
  }
  public static void build(DataStream<String> raw, JobConfig config, Path configRoot, String bootstrap, String warehouse) throws Exception {
    String schema = Files.readString(configRoot.resolve(config.envelopeSchema()));
    var validated = raw.process(new Transforms.Validate(schema)).name("envelope JSON Schema validation").uid("validate-envelope");
    var errors = new ArrayList<DataStream<String>>();
    errors.add(validated.getSideOutput(Transforms.ERRORS));
    // Preserve source-native partition watermarks. Reassigning after merging Kafka
    // partitions can incorrectly declare buffered records late after a restore.
    var events = validated;
    output(events.map(Transforms::event).returns(Types.STRING).uid("project-events"), "events", config, configRoot, bootstrap, warehouse, errors);
    output(events.map(v -> Transforms.node(v, config.graph())).returns(Types.STRING).uid("project-nodes"), "nodes", config, configRoot, bootstrap, warehouse, errors);
    var edges = events.process(new org.apache.flink.streaming.api.functions.ProcessFunction<String,String>() {
      @Override public void processElement(String value, Context context, Collector<String> out) {
        try { String edge = Transforms.edge(value, config.graph()); if (edge != null) out.collect(edge); }
        catch (RuntimeException invalid) { context.output(Transforms.ERRORS, Json.error(value, "Invalid graph target: " + invalid.getMessage())); }
      }
    }).uid("project-edges");
    errors.add(edges.getSideOutput(Transforms.ERRORS));
    output(edges, "edges", config, configRoot, bootstrap, warehouse, errors);
    DataStream<String> allMetrics = null;
    for (var aggregation : config.aggregations()) {
      var samples = events.process(new Transforms.Samples(aggregation)).uid("samples-" + aggregation.id());
      errors.add(samples.getSideOutput(Transforms.ERRORS));
      var metrics = samples.keyBy(Transforms::sampleKey)
          .window(TumblingEventTimeWindows.of(Duration.ofSeconds(aggregation.windowSeconds())))
          .sideOutputLateData(Transforms.LATE).aggregate(new Transforms.Stats(), new Transforms.FinishWindow())
          .uid("window-" + aggregation.id());
      errors.add(metrics.getSideOutput(Transforms.LATE).map(v -> Json.error(v, "Late beyond event-time watermark: " + aggregation.id())).returns(Types.STRING));
      allMetrics = allMetrics == null ? metrics : allMetrics.union(metrics);
    }
    if (allMetrics != null) output(allMetrics, "metrics", config, configRoot, bootstrap, warehouse, errors);
    DataStream<String> allErrors = errors.getFirst();
    for (int i = 1; i < errors.size(); i++) allErrors = allErrors.union(errors.get(i));
    var errorValidator = new Json.Validator(Files.readString(configRoot.resolve("schemas/error.json")));
    allErrors.map(v -> { errorValidator.validate(v); return v; }).returns(Types.STRING).uid("validate-errors")
        .sinkTo(kafka(bootstrap, config.errorTopic(), "errors")).name("Kafka errors").uid("kafka-errors");
  }
  private static void output(DataStream<String> data, String kind, JobConfig config, Path root,
      String bootstrap, String warehouse, ArrayList<DataStream<String>> errors) throws Exception {
    var output = config.outputs().get(kind);
    DataStream<String> checked = data;
    if (output.schema() != null) {
      // Output records have different timestamp names from input envelopes.
      var validator = new Json.Validator(Files.readString(root.resolve(output.schema())));
      var valid = data.process(new org.apache.flink.streaming.api.functions.ProcessFunction<String, String>() {
        @Override public void processElement(String value, Context context, Collector<String> collector) {
          try { validator.validate(value); collector.collect(value); }
          catch (Exception ex) { context.output(Transforms.ERRORS, Json.error(value, "Output " + kind + ": " + ex.getMessage())); }
        }
      }).uid("validate-" + kind);
      errors.add(valid.getSideOutput(Transforms.ERRORS));
      checked = valid;
    }
    if (output.table() != null) IcebergTables.attach(checked, kind, output.table(), config.namespace(), warehouse, config.parallelism());
    if (output.topic() != null) checked.sinkTo(kafka(bootstrap, output.topic(), kind)).name("Kafka " + kind).uid("kafka-" + kind);
  }
  static KafkaSink<String> kafka(String bootstrap, String topic, String kind) {
    return KafkaSink.<String>builder().setBootstrapServers(bootstrap).setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
        .setKafkaProducerConfig(KafkaSecurity.properties())
        .setTransactionalIdPrefix(env("JOB_ID", "context-secure-v1") + "-" + kind + "-")
        .setProperty("transaction.timeout.ms", "900000")
        .setRecordSerializer((KafkaRecordSerializationSchema<String>) (value, context, timestamp) -> {
          String key = kafkaKey(value, kind);
          return new ProducerRecord<>(topic, null, timestamp, key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
        }).build();
  }
  public static String kafkaKey(String value, String kind) {
    var n = Json.read(value);
    if (kind.equals("errors")) return n.path("errorId").asText();
    String workspace = n.path("workspace_id").asText();
    String resource = n.path("resource_id").asText();
    String entity = n.path("entity_id").asText(n.path("node_id").asText(n.path("source_id").asText()));
    if (!resource.equals(Scope.resource(workspace, entity))) throw new IllegalArgumentException("Invalid output resource label");
    if (kind.equals("edges") && !n.path("target_resource_id").asText().equals(Scope.resource(workspace,n.path("target_id").asText())))
      throw new IllegalArgumentException("Invalid edge target label");
    return workspace + "\u0000" + resource;
  }
}
