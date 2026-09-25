package io.contextgraph.query;

import io.contextgraph.security.*;
import java.time.Instant;
import java.util.Map;

final class EventAuthorization {
  static boolean visible(LiveBus.Event event,SecurityContext context,PermissionChecks permissions,String requestedEntity) {
    if(Instant.now().getEpochSecond()>=context.expiresAtEpochSecond())throw new AuthException(401,"Expired session");
    permissions.workspace(context,"access");
    Map<String,Object> payload=event.payload();
    Map<?,?> security=payload.get("security") instanceof Map<?,?> value?value:payload;
    Object workspace=security.get(payload.containsKey("security")?"workspaceId":"workspace_id");
    if(!context.workspaceId().equals(workspace))return false;
    String resourceField=payload.containsKey("security")?"resourceId":"resource_id";
    Object resource=security.get(resourceField);
    Object entity=payload.getOrDefault("entityId",payload.get("entity_id"));
    IcebergQueries.validateLabel(context,resource,entity);
    if(!permissions.allowed(context,(String)resource)) {
      if(requestedEntity!=null)throw new AuthException(403,"Access revoked");
      return false;
    }
    return true;
  }
}
