package io.contextgraph.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.contextgraph.security.AccessControl;

/** Mandatory provenance from the authenticated ingestion principal. Never configurable. */
public final class Scope {
  private Scope() {}
  public static JsonNode validate(JsonNode event) {
    var labels = event.path("security");
    String workspace = labels.path("workspaceId").asText("");
    String resource = labels.path("resourceId").asText("");
    String subject = labels.path("subjectId").asText("");
    if (event.path("schemaVersion").asInt() != 2 || subject.isBlank()
        || !resource.equals(AccessControl.resourceId(workspace, event.path("entityId").asText(""))))
      throw new IllegalArgumentException("Missing or forged security provenance");
    return event;
  }
  public static ObjectNode row(JsonNode event) {
    validate(event);
    return Json.object().put("workspace_id", event.path("security").path("workspaceId").asText())
        .put("resource_id", event.path("security").path("resourceId").asText());
  }
  public static String resource(String workspace, String entity) { return AccessControl.resourceId(workspace, entity); }
}
