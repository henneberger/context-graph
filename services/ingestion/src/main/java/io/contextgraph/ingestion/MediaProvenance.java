package io.contextgraph.ingestion;
import io.contextgraph.security.*;
import io.vertx.core.json.JsonObject;
import java.nio.file.*;

/** Server-owned media sidecars; absence and inconsistent labels always deny. */
final class MediaProvenance {
    static JsonObject labels(SecurityContext context,String entityId) {
        return new JsonObject().put("workspaceId",context.workspaceId()).put("resourceId",AccessControl.resourceId(context.workspaceId(),entityId)).put("subjectId",context.subjectId());
    }
    static Path sidecar(Path root,Path file) { return file.getParent().equals(root) ? Path.of(file+".security.json") : file.getParent().resolve(".security.json"); }
    static void validate(JsonObject data) {
        JsonObject labels=data.getJsonObject("security");
        if(labels==null || !AccessControl.resourceId(labels.getString("workspaceId"),data.getString("entityId")).equals(labels.getString("resourceId")) || labels.getString("subjectId","").isBlank()) throw new AuthException(403,"Media unavailable");
    }
    static JsonObject read(Path root,Path file) {
        try {
            if(!file.toRealPath().startsWith(root.toRealPath()) || !Files.isRegularFile(file)) throw new AuthException(403,"Media unavailable");
            Path sidecar=sidecar(root,file);
            if(!sidecar.toRealPath().startsWith(root.toRealPath()) || Files.size(sidecar)>8192) throw new AuthException(403,"Media unavailable");
            JsonObject data=new JsonObject(Files.readString(sidecar)); validate(data);
            return data;
        } catch(AuthException e) { throw e; } catch(Exception e) { throw new AuthException(403,"Media unavailable"); }
    }
}
