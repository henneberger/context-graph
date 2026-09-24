package io.contextgraph.ingestion;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.*;
import io.vertx.core.json.JsonObject;
import java.nio.file.*;

public final class SchemaCheck {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final JsonSchema schema;
    public SchemaCheck(Path path) throws Exception {
        try (var in = Files.newInputStream(path)) {
            schema = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012).getSchema(in, SchemaValidatorsConfig.builder().formatAssertionsEnabled(true).build());
        }
    }
    public void validate(JsonObject value) throws Exception {
        var errors = schema.validate(MAPPER.readTree(value.encode()));
        if (!errors.isEmpty()) throw new IllegalArgumentException(errors.toString());
    }
}
