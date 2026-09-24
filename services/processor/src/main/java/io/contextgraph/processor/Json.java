package io.contextgraph.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import java.io.Serializable;
import java.time.Instant;

public final class Json {
  public static final ObjectMapper MAPPER = new ObjectMapper();
  public static JsonNode read(String s) {
    try { return MAPPER.readTree(s); } catch (Exception e) { throw new IllegalArgumentException("Invalid JSON", e); }
  }
  public static ObjectNode object() { return MAPPER.createObjectNode(); }
  public static String error(String raw, String reason) {
    return object().put("errorId", java.util.UUID.randomUUID().toString())
        .put("endpoint", "processor").put("message", reason + "; record=" + (raw.length() > 8192 ? raw.substring(0, 8192) : raw))
        .put("occurredAt", Instant.now().toString()).toString();
  }
  public static class Validator implements Serializable {
    private final String definition;
    private transient JsonSchema schema;
    public Validator(String definition) { this.definition = definition; }
    public JsonNode validate(String value) {
      if (schema == null) schema = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012).getSchema(definition);
      JsonNode node = read(value);
      var errors = schema.validate(node);
      if (!errors.isEmpty()) throw new IllegalArgumentException(errors.toString());
      return node;
    }
  }
}
