package io.contextgraph.security;
public record SecurityContext(String subjectId, String workspaceId, long expiresAtEpochSecond) {}
