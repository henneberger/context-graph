package io.contextgraph.processor;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

@EnabledIfEnvironmentVariable(named="RUN_KAFKA_IT",matches="true")
class KafkaIntegrationTest {
  @TempDir Path temp;
  static Properties props(String broker,String... pairs) {
    var p=io.contextgraph.security.KafkaSecurity.properties();p.put("bootstrap.servers",broker);
    for(int i=0;i<pairs.length;i+=2)p.put(pairs[i],pairs[i+1]);return p;
  }
  @Test void schemaRowsCommitToKafkaAndIceberg() throws Exception {
    String broker=System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS","localhost:19092");
    String prefix="cg.secure.it-"+UUID.randomUUID();var topics=List.of(prefix+"-input",prefix+"-output",prefix+"-errors");
    String definition="{\"type\":\"object\",\"properties\":{\"value\":{\"type\":\"integer\",\"minimum\":0,\"maximum\":100}}}";
    var mapping=new JsonSchemaRows(definition);
    var schema=new org.apache.iceberg.Schema(org.apache.iceberg.flink.FlinkSchemaUtil.convert(SqlBundleJob.sourceType(mapping).getLogicalType()).asStructType().fields());
    try(Admin admin=Admin.create(props(broker))) {
      admin.createTopics(topics.stream().map(t->new NewTopic(t,1,(short)1)).toList()).all().get(30,TimeUnit.SECONDS);
      try {
        var settings=new org.apache.flink.configuration.Configuration();settings.setString("execution.checkpointing.dir",temp.resolve("checkpoints").toUri().toString());
        var env=StreamExecutionEnvironment.getExecutionEnvironment(settings);env.setParallelism(1);env.enableCheckpointing(500);
        String envelope=Files.readString(Path.of("../../config/schemas/envelope.json"));
        var source=KafkaSource.<String>builder().setBootstrapServers(broker).setProperties(props(broker)).setTopics(topics.get(0)).setGroupId(prefix)
          .setStartingOffsets(OffsetsInitializer.earliest()).setValueOnlyDeserializer(new SimpleStringSchema()).build();
        var valid=env.fromSource(source,new EventTime(envelope,0,1),"input").process(new Transforms.Validate(envelope));
        var rows=valid.map(v->GenericIceberg.encode(SqlBundleJob.input(v,mapping),schema,"workspace-a")).returns(Types.STRING);
        String warehouse=temp.resolve("warehouse").toUri().toString();
        GenericIceberg.attach(rows,schema,"samples","test",1,"samples",warehouse);
        rows.sinkTo(RuntimeSupport.kafka(broker,topics.get(1),prefix));
        valid.getSideOutput(Transforms.ERRORS).sinkTo(RuntimeSupport.kafka(broker,topics.get(2),"errors"));
        var job=env.executeAsync("JSON Schema transport");
        try {
          try(var producer=new KafkaProducer<String,String>(props(broker,"key.serializer","org.apache.kafka.common.serialization.StringSerializer","value.serializer","org.apache.kafka.common.serialization.StringSerializer"))) {
            producer.send(new ProducerRecord<>(topics.get(0),"key",ProcessorTest.EVENT)).get(20,TimeUnit.SECONDS);
            producer.send(new ProducerRecord<>(topics.get(0),"bad","{}" )).get(20,TimeUnit.SECONDS);
          }
          var seen=new HashSet<String>();
          try(var consumer=new KafkaConsumer<String,String>(props(broker,"group.id",prefix+"-read","auto.offset.reset","earliest","isolation.level","read_committed","key.deserializer","org.apache.kafka.common.serialization.StringDeserializer","value.deserializer","org.apache.kafka.common.serialization.StringDeserializer"))) {
            consumer.subscribe(topics.subList(1,3));long deadline=System.nanoTime()+Duration.ofSeconds(75).toNanos();
            while(seen.size()<2&&System.nanoTime()<deadline)for(var record:consumer.poll(Duration.ofMillis(250))) {
              seen.add(record.topic());if(record.topic().equals(topics.get(1))) {
                var row=Json.read(record.value());assertEquals(4,row.path("value").asInt());
                assertTrue(row.path("_json_remainder").path("arbitrary").path("retained").asBoolean());
              }
            }
          }
          assertEquals(new HashSet<>(topics.subList(1,3)),seen);
          var table=IcebergTables.loader(warehouse).loadCatalog().loadTable(org.apache.iceberg.catalog.TableIdentifier.of("test","samples"));
          long deadline=System.nanoTime()+Duration.ofSeconds(20).toNanos();while(table.currentSnapshot()==null&&System.nanoTime()<deadline){Thread.sleep(100);table.refresh();}
          assertNotNull(table.currentSnapshot());
        }finally{job.cancel().get(30,TimeUnit.SECONDS);}
      }finally{admin.deleteTopics(topics).all().get(30,TimeUnit.SECONDS);}
    }
  }
}
