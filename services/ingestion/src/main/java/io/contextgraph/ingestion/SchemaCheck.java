package io.contextgraph.ingestion;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.*;
import io.vertx.core.json.JsonObject;
import java.nio.file.*;

public final class SchemaCheck {
    private static final ObjectMapper MAPPER = new ObjectMapper().enable(com.fasterxml.jackson.databind.DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    private final JsonSchema schema;
    public SchemaCheck(Path path) throws Exception {
        try (var in = Files.newInputStream(path)) {
            schema = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012).getSchema(in, SchemaValidatorsConfig.builder().formatAssertionsEnabled(true).build());
        }
    }
    public void validate(Object value) throws Exception {
        var errors = schema.validate(MAPPER.readTree(io.vertx.core.json.Json.encode(value)));
        if (!errors.isEmpty()) throw new IllegalArgumentException(errors.toString());
    }
}
