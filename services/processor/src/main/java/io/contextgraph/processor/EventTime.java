package io.contextgraph.processor;

import java.time.Duration;
import java.time.Instant;
import org.apache.flink.api.common.eventtime.*;

/** KafkaSource creates one generator per partition, before partition streams are merged. */
public final class EventTime implements WatermarkStrategy<String> {
  private final String schema;
  private final long delaySeconds;
  private final long idleSeconds;
  public EventTime(String schema, long delaySeconds, long idleSeconds) {
    this.schema = schema; this.delaySeconds = delaySeconds; this.idleSeconds = idleSeconds;
  }
  @Override public TimestampAssigner<String> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
    var validator = new Json.Validator(schema);
    return (value, kafkaTimestamp) -> {
      try { return Instant.parse(Scope.validate(validator.validate(value)).path("eventTime").asText()).toEpochMilli(); }
      catch (RuntimeException invalid) { return Long.MIN_VALUE; }
    };
  }
  @Override public WatermarkGenerator<String> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
    var validEvents = new WatermarksWithIdleness<String>(
        new BoundedOutOfOrdernessWatermarks<>(Duration.ofSeconds(delaySeconds)),
        Duration.ofSeconds(idleSeconds), context.getInputActivityClock());
    return new WatermarkGenerator<>() {
      @Override public void onEvent(String value, long timestamp, WatermarkOutput output) {
        // Invalid events still reach the downstream validator/error queue, but may
        // neither advance event time nor keep an invalid-only partition active.
        if (timestamp != Long.MIN_VALUE) validEvents.onEvent(value, timestamp, output);
      }
      @Override public void onPeriodicEmit(WatermarkOutput output) { validEvents.onPeriodicEmit(output); }
    };
  }
}
