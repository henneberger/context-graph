package io.contextgraph.security;
/** Deliberately contains no credential or policy details suitable for disclosure. */
public final class AuthException extends RuntimeException {
    private final int status;
    public AuthException(int status, String message) { super(message); this.status=status; }
    public int status() { return status; }
}
