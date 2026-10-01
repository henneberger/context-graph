package io.contextgraph.processor;

import java.util.ArrayList;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/** Lossless Kafka transport, including duplicate headers and null-valued tombstones. */
public class LakeRecord implements java.io.Serializable {
  public String topic;
  public int partition;
  public long offset;
  public long timestamp;
  public String timestampType;
  public byte[] key;
  public byte[] value;
  public String[] headerNames;
  public byte[][] headerValues;

  public LakeRecord() {}

  public static class Deserializer implements KafkaRecordDeserializationSchema<LakeRecord> {
    @Override public void deserialize(ConsumerRecord<byte[], byte[]> input, Collector<LakeRecord> out) {
      LakeRecord r = new LakeRecord();
      r.topic = input.topic(); r.partition = input.partition(); r.offset = input.offset();
      r.timestamp = input.timestamp(); r.timestampType = input.timestampType().name();
      r.key = input.key(); r.value = input.value();
      var names = new ArrayList<String>(); var values = new ArrayList<byte[]>();
      input.headers().forEach(h -> { names.add(h.key()); values.add(h.value()); });
      r.headerNames = names.toArray(String[]::new); r.headerValues = values.toArray(byte[][]::new);
      out.collect(r);
    }
    @Override public TypeInformation<LakeRecord> getProducedType() { return TypeInformation.of(LakeRecord.class); }
  }
}
