package io.contextgraph.security;
public record SecurityContext(String subjectId, String workspaceId, long expiresAtEpochSecond, boolean writeAllowed) {
    public SecurityContext(String subjectId,String workspaceId,long expiresAtEpochSecond) { this(subjectId,workspaceId,expiresAtEpochSecond,true); }
}
