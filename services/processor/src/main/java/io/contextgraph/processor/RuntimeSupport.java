package io.contextgraph.processor;
import java.nio.charset.StandardCharsets;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import io.contextgraph.security.KafkaSecurity;
/** Transport bindings shared by schema-defined jobs. No domain model. */
public final class RuntimeSupport {
  public static String env(String name,String fallback){return System.getenv().getOrDefault(name,fallback);}
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
    String entity = n.path("entity_id").asText();
    if (!resource.equals(Scope.resource(workspace, entity))) throw new IllegalArgumentException("Invalid output resource label");
    return workspace + "\u0000" + resource;
  }
}
