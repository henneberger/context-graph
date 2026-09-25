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
}
