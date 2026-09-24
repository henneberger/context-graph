package io.contextgraph.processor;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

@EnabledIfEnvironmentVariable(named="RUN_KAFKA_IT", matches="true")
class KafkaIntegrationTest {
  @TempDir Path temp;
  @Test void commitsKafkaAndIcebergAtCheckpoint() throws Exception {
    String broker = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092");
    String prefix = "cg.secure.it-" + UUID.randomUUID();
    List<String> topics = List.of(prefix+"-source",prefix+"-nodes",prefix+"-edges",prefix+"-metrics",prefix+"-errors");
    var base = JobConfig.load(Path.of("../../config/jobs.yaml"));
    var outputs = Map.of("events",new JobConfig.Output("events",null,null),
        "nodes",new JobConfig.Output("nodes",topics.get(1),"schemas/nodes.schema.json"),
        "edges",new JobConfig.Output("edges",topics.get(2),"schemas/edges.schema.json"),
        "metrics",new JobConfig.Output("metrics",topics.get(3),"schemas/metrics.schema.json"));
    JobConfig c = new JobConfig(prefix,1,List.of(topics.getFirst()),prefix,"schemas/envelope.json",topics.get(4),
        500,100,0,1,"context_secure",outputs,base.graph(),base.aggregations());
    String warehouse = temp.resolve("warehouse").toUri().toString();
    try (Admin admin = Admin.create(clientConfig(broker))) {
      admin.createTopics(topics.stream().map(t -> new NewTopic(t,t.equals(topics.getFirst()) ? 2 : 1,(short)1)).toList()).all().get(30,TimeUnit.SECONDS);
      try {
        var settings = new Configuration();
        settings.setString("state.backend.type","rocksdb");
        settings.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS,true);
        settings.set(CheckpointingOptions.CHECKPOINT_STORAGE,"filesystem");
        settings.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY,temp.resolve("checkpoints").toUri().toString());
        var env = StreamExecutionEnvironment.getExecutionEnvironment(settings);
        env.setParallelism(1);
        env.enableCheckpointing(500);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(100);
        var source = KafkaSource.<String>builder().setBootstrapServers(broker).setTopics(topics.getFirst())
            .setProperties(io.contextgraph.security.KafkaSecurity.properties()).setGroupId(prefix).setStartingOffsets(OffsetsInitializer.earliest()).setValueOnlyDeserializer(new SimpleStringSchema()).build();
        var watermarks = new EventTime(java.nio.file.Files.readString(Path.of("../../config/schemas/envelope.json")),0,1);
        ContextGraphJob.build(env.fromSource(source,watermarks,"test kafka"),c,Path.of("../../config"),broker,warehouse);
        var job = env.executeAsync("Kafka checkpoint compatibility");
        try {
          try (var producer = new KafkaProducer<String,String>(clientConfig(broker,"key.serializer","org.apache.kafka.common.serialization.StringSerializer","value.serializer","org.apache.kafka.common.serialization.StringSerializer"))) {
            // Newer partition is published first. A downstream/global watermark
            // would let its timestamp make the older partition's data late.
            producer.send(new ProducerRecord<>(topics.getFirst(),1,"sensor-1",ProcessorTest.EVENT.replace("18:00:01Z","18:00:21Z"))).get(20,TimeUnit.SECONDS);
            producer.send(new ProducerRecord<>(topics.getFirst(),0,"sensor-1",ProcessorTest.EVENT)).get(20,TimeUnit.SECONDS);
            producer.send(new ProducerRecord<>(topics.getFirst(),"invalid","{}" )).get(20,TimeUnit.SECONDS);
          }
          Set<String> seen = new HashSet<>();
          var observedErrors = new ArrayList<String>();
          var observedMetrics = new ArrayList<String>();
          try (var consumer = new KafkaConsumer<String,String>(clientConfig(broker,"group.id",prefix+"-assert","auto.offset.reset","earliest","isolation.level","read_committed","key.deserializer","org.apache.kafka.common.serialization.StringDeserializer","value.deserializer","org.apache.kafka.common.serialization.StringDeserializer"))) {
            consumer.subscribe(topics.subList(1,5));
            long deadline = System.nanoTime()+Duration.ofSeconds(75).toNanos();
            while (System.nanoTime()<deadline && seen.size()<4) {
              for (var record : consumer.poll(Duration.ofMillis(250))) {
                seen.add(record.topic());
                if (record.topic().equals(topics.get(4))) observedErrors.add(record.value());
                if (record.topic().equals(topics.get(3))) observedMetrics.add(record.value());
              }
              if (job.getJobExecutionResult().isDone()) job.getJobExecutionResult().get();
            }
          }
          assertEquals(new HashSet<>(topics.subList(1,5)),seen,"Every output, including invalid input, must become read_committed visible");
          assertTrue(observedErrors.stream().noneMatch(e -> Json.read(e).path("message").asText().contains("Late beyond")), "Merged source partitions must not mark valid backlog late");
          assertTrue(observedMetrics.stream().anyMatch(e -> Json.read(e).path("window_start").asText().equals("2026-09-24T18:00:00Z")), "Older partition's window must be emitted");
          var table = IcebergTables.loader(warehouse).loadCatalog().loadTable(org.apache.iceberg.catalog.TableIdentifier.of("context_secure","events"));
          long deadline = System.nanoTime()+Duration.ofSeconds(15).toNanos();
          while (table.currentSnapshot()==null && System.nanoTime()<deadline) { Thread.sleep(100); table.refresh(); }
          assertNotNull(table.currentSnapshot());
          assertEquals("2",table.currentSnapshot().summary().get("total-records"));
          String savepoint = job.stopWithSavepoint(false,temp.resolve("savepoints").toUri().toString(),
              org.apache.flink.core.execution.SavepointFormatType.CANONICAL).get(45,TimeUnit.SECONDS);
          assertTrue(java.nio.file.Files.exists(Path.of(java.net.URI.create(savepoint)).resolve("_metadata")));
          settings.setString("execution.state-recovery.path", savepoint);
          var restored = StreamExecutionEnvironment.getExecutionEnvironment(settings);
          restored.setParallelism(1);
          restored.enableCheckpointing(500);
          restored.getCheckpointConfig().setMinPauseBetweenCheckpoints(100);
          ContextGraphJob.build(restored.fromSource(source,watermarks,"test kafka"),c,Path.of("../../config"),broker,warehouse);
          var resumed = restored.executeAsync("Kafka checkpoint compatibility restored");
          try {
            try (var producer = new KafkaProducer<String,String>(clientConfig(broker,"key.serializer","org.apache.kafka.common.serialization.StringSerializer","value.serializer","org.apache.kafka.common.serialization.StringSerializer"))) {
              producer.send(new ProducerRecord<>(topics.getFirst(),"sensor-1",ProcessorTest.EVENT.replace("18:00:01Z","18:00:41Z"))).get(20,TimeUnit.SECONDS);
            }
            long restoreDeadline = System.nanoTime()+Duration.ofSeconds(45).toNanos();
            do {
              Thread.sleep(200); table.refresh();
              if (resumed.getJobExecutionResult().isDone()) resumed.getJobExecutionResult().get();
            } while (!"3".equals(table.currentSnapshot().summary().get("total-records")) && System.nanoTime()<restoreDeadline);
            assertEquals("3",table.currentSnapshot().summary().get("total-records"),"Restore must retain source offsets without replay duplicates");
          } finally { cancelAndWait(resumed); }
        } finally {
          if (!job.getJobExecutionResult().isDone()) cancelAndWait(job);
        }
      } finally { admin.deleteTopics(topics).all().get(20,TimeUnit.SECONDS); }
    }
  }
  private static java.util.Properties clientConfig(String broker, String... options) {
    var properties = io.contextgraph.security.KafkaSecurity.properties();
    properties.put("bootstrap.servers",broker);
    for (int i=0;i<options.length;i+=2) properties.put(options[i],options[i+1]);
    return properties;
  }
  private static void cancelAndWait(org.apache.flink.core.execution.JobClient job) throws Exception {
    job.cancel().get(20,TimeUnit.SECONDS);
    try { job.getJobExecutionResult().get(20,TimeUnit.SECONDS); }
    catch (java.util.concurrent.ExecutionException expectedCancellation) {
      Throwable cause = expectedCancellation;
      while (cause.getCause() != null) cause = cause.getCause();
      if (!(cause instanceof org.apache.flink.runtime.client.JobCancellationException)) throw expectedCancellation;
    }
  }
}
