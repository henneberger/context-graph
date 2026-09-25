package io.contextgraph.query;

import io.contextgraph.security.*;
import java.net.*;
import java.net.http.*;
import java.time.Duration;
import java.util.*;

/** Mutations reuse the authenticated ingestion endpoint; no raw Kafka or warehouse writes. */
final class EventMutations {
  private static final HttpClient HTTP=HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).followRedirects(HttpClient.Redirect.NEVER).build();
  static Map<String,Object> publish(String path,Map<String,Object> input,String authorization,SecurityContext context,AccessControl access)throws Exception {
    String origin=System.getenv("BUNDLE_INGESTION_URL");
    if(origin==null||!origin.startsWith("https://")||!path.matches("/ingest/[a-z][a-z0-9_]*"))throw new IllegalStateException("Mutation ingestion binding unavailable");
    Object entity=input.get("entityId");if(!(entity instanceof String id))throw new SecurityException("Entity required");
    access.require(context,AccessControl.resourceId(context.workspaceId(),id),"ingest");
    var request=HttpRequest.newBuilder(URI.create(origin+path)).timeout(Duration.ofSeconds(40)).header("Authorization",authorization)
      .header("X-Workspace-Id",context.workspaceId()).header("Content-Type","application/json")
      .POST(HttpRequest.BodyPublishers.ofString(IcebergQueries.JSON.writeValueAsString(input))).build();
    var response=HTTP.send(request,HttpResponse.BodyHandlers.ofString());if(response.statusCode()!=202)throw new IllegalStateException("Command rejected or unavailable");
    var body=IcebergQueries.JSON.readTree(response.body());if(!body.path("eventId").asText().matches("[a-f0-9-]{36}"))throw new IllegalStateException("Invalid receipt");
    return Map.of("eventId",body.path("eventId").asText(),"status","ACCEPTED");
  }
}
