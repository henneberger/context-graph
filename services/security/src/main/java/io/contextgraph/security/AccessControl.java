package io.contextgraph.security;

import com.fasterxml.jackson.databind.*;
import com.nimbusds.jose.*;
import com.nimbusds.jose.crypto.RSASSAVerifier;
import com.nimbusds.jose.jwk.*;
import com.nimbusds.jwt.SignedJWT;
import java.net.URI;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.*;
import java.util.*;

/** Verified identity and fully-consistent, fail-closed SpiceDB authorization. Blocking: use workers. */
public final class AccessControl {
    private static final ObjectMapper JSON=new ObjectMapper();
    private final String issuer,audience,spiceToken;
    private final URI jwksUri,spiceUri;
    private final HttpClient http;
    private volatile JWKSet keys;
    private volatile long keysFetchedAt;
    private long lastForcedRefresh;
    public static AccessControl fromEnvironment() { return new AccessControl(System.getenv()); }
    AccessControl(Map<String,String> env) {
        boolean allowHttp="true".equals(env.get("SECURITY_ALLOW_HTTP"));
        issuer=required(env,"OIDC_ISSUER"); audience=required(env,"OIDC_AUDIENCE"); spiceToken=required(env,"SPICEDB_TOKEN");
        endpoint(issuer,allowHttp);
        jwksUri=endpoint(required(env,"OIDC_JWKS_URL"),allowHttp);
        spiceUri=endpoint(required(env,"SPICEDB_ENDPOINT").replaceAll("/+$","")+"/v1/permissions/check",allowHttp);
        http=HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).followRedirects(HttpClient.Redirect.NEVER).build();
    }
    private static String required(Map<String,String> env,String key) { String value=env.get(key); if(value==null || value.isBlank()) throw new IllegalStateException(key+" is required"); return value; }
    private static URI endpoint(String value,boolean allowHttp) {
        URI uri=URI.create(value);
        if(uri.getHost()==null || uri.getUserInfo()!=null || uri.getFragment()!=null || !("https".equals(uri.getScheme()) || allowHttp && "http".equals(uri.getScheme()))) throw new IllegalStateException("Security endpoints require HTTPS; HTTP requires explicit development setting");
        return uri;
    }
    public SecurityContext authenticate(String authorization,String workspaceId) {
        validateWorkspace(workspaceId);
        if(authorization==null || !authorization.startsWith("Bearer ") || authorization.length()>16384) throw new AuthException(401,"Valid Bearer credentials required");
        SecurityContext context;
        try {
            SignedJWT jwt=SignedJWT.parse(authorization.substring(7));
            if(!JWSAlgorithm.RS256.equals(jwt.getHeader().getAlgorithm()) || jwt.getHeader().getCriticalParams()!=null && !jwt.getHeader().getCriticalParams().isEmpty()) throw new AuthException(401,"Unsupported token");
            String kid=jwt.getHeader().getKeyID();
            if(kid==null || kid.isBlank()) throw new AuthException(401,"Token key is required");
            JWK key=key(kid);
            if(!(key instanceof RSAKey rsa) || rsa.isPrivate() || rsa.getKeyUse()!=null && !KeyUse.SIGNATURE.equals(rsa.getKeyUse()) || rsa.getAlgorithm()!=null && !JWSAlgorithm.RS256.equals(rsa.getAlgorithm()) || rsa.size()<2048 || !jwt.verify(new RSASSAVerifier(rsa.toRSAPublicKey()))) throw new AuthException(401,"Invalid token signature");
            var claims=jwt.getJWTClaimsSet(); long now=Instant.now().getEpochSecond();
            if(!issuer.equals(claims.getIssuer()) || claims.getAudience()==null || !claims.getAudience().contains(audience) || claims.getExpirationTime()==null || claims.getExpirationTime().toInstant().getEpochSecond()<=now || claims.getNotBeforeTime()!=null && claims.getNotBeforeTime().toInstant().getEpochSecond()>now || claims.getSubject()==null || claims.getSubject().isBlank() || claims.getSubject().equals("*") || claims.getSubject().length()>256 || claims.getSubject().chars().anyMatch(Character::isISOControl)) throw new AuthException(401,"Invalid token claims");
            context=new SecurityContext(claims.getSubject(),workspaceId,claims.getExpirationTime().toInstant().getEpochSecond());
        } catch(AuthException e) { throw e; }
        catch(Exception e) { throw new AuthException(401,"Invalid token"); }
        requireWorkspace(context,"access"); return context;
    }
    private JWK key(String kid) {
        JWKSet snapshot=keys;
        if(snapshot==null || System.currentTimeMillis()-keysFetchedAt>300000) snapshot=refresh(false);
        JWK key=snapshot.getKeyByKeyId(kid);
        if(key==null) key=refresh(true).getKeyByKeyId(kid);
        if(key==null) throw new AuthException(401,"Unknown token key");
        return key;
    }
    private synchronized JWKSet refresh(boolean forced) {
        long now=System.currentTimeMillis();
        if(keys!=null && (!forced && now-keysFetchedAt<300000 || forced && now-lastForcedRefresh<30000)) return keys;
        if(forced) lastForcedRefresh=now;
        try {
            var response=sendBounded(HttpRequest.newBuilder(jwksUri).timeout(Duration.ofSeconds(5)).GET().build(),1024*1024);
            if(response.statusCode()!=200) throw new IllegalStateException();
            JWKSet parsed=JWKSet.parse(new String(response.body(),StandardCharsets.UTF_8));
            if(parsed.getKeys().isEmpty()) throw new IllegalStateException();
            keys=parsed; keysFetchedAt=now; return parsed;
        } catch(Exception e) { if(e instanceof InterruptedException) Thread.currentThread().interrupt(); throw new AuthException(503,"Identity verification unavailable"); }
    }
    public void requireWorkspace(SecurityContext context,String permission) {
        checkExpiry(context); validateWorkspace(context.workspaceId());
        if(!Set.of("access","manage","view_schema").contains(permission)) throw new AuthException(403,"Permission denied");
        if(!check(context,"workspace",context.workspaceId(),permission)) throw new AuthException(403,"Permission denied");
    }
    public boolean allowed(SecurityContext context,String resourceId,String permission) {
        requireWorkspace(context,"access");
        if(resourceId==null || !resourceId.matches("[a-f0-9]{64}") || !Set.of("view","ingest").contains(permission)) throw new AuthException(403,"Permission denied");
        return check(context,"entity",resourceId,permission);
    }
    public void require(SecurityContext context,String resourceId,String permission) { if(!allowed(context,resourceId,permission)) throw new AuthException(403,"Permission denied"); }
    private static void checkExpiry(SecurityContext context) { if(context==null || context.expiresAtEpochSecond()<=Instant.now().getEpochSecond()) throw new AuthException(401,"Credentials expired"); }
    private boolean check(SecurityContext context,String type,String id,String permission) {
        checkExpiry(context);
        try {
            String body=JSON.writeValueAsString(Map.of("consistency",Map.of("fully_consistent",true),"resource",Map.of("object_type",type,"object_id",id),"permission",permission,"subject",Map.of("object",Map.of("object_type","user","object_id",context.subjectId()))));
            var request=HttpRequest.newBuilder(spiceUri).timeout(Duration.ofSeconds(5)).header("Authorization","Bearer "+spiceToken).header("Content-Type","application/json").POST(HttpRequest.BodyPublishers.ofString(body)).build();
            var response=sendBounded(request,65536);
            if(response.statusCode()!=200) throw new AuthException(503,"Authorization unavailable");
            String result=JSON.readTree(response.body()).path("permissionship").asText();
            if("PERMISSIONSHIP_HAS_PERMISSION".equals(result)) { checkExpiry(context); return true; }
            if("PERMISSIONSHIP_NO_PERMISSION".equals(result) || "PERMISSIONSHIP_CONDITIONAL_PERMISSION".equals(result)) return false;
            throw new AuthException(503,"Authorization unavailable");
        } catch(AuthException e) { throw e; }
        catch(Exception e) { if(e instanceof InterruptedException) Thread.currentThread().interrupt(); throw new AuthException(503,"Authorization unavailable"); }
    }
    private HttpResponse<byte[]> sendBounded(HttpRequest request,int limit) throws Exception {
        var future=http.sendAsync(request,info -> new LimitedBody(limit));
        try { return future.get(5,java.util.concurrent.TimeUnit.SECONDS); }
        finally { if(!future.isDone()) future.cancel(true); }
    }
    private static final class LimitedBody implements HttpResponse.BodySubscriber<byte[]> {
        private final HttpResponse.BodySubscriber<byte[]> delegate=HttpResponse.BodySubscribers.ofByteArray();
        private final int limit; private long received; private java.util.concurrent.Flow.Subscription subscription;
        LimitedBody(int limit) { this.limit=limit; }
        public java.util.concurrent.CompletionStage<byte[]> getBody() { return delegate.getBody(); }
        public void onSubscribe(java.util.concurrent.Flow.Subscription subscription) { this.subscription=subscription; delegate.onSubscribe(subscription); }
        public void onNext(java.util.List<java.nio.ByteBuffer> buffers) {
            for(var buffer:buffers) received+=buffer.remaining();
            if(received>limit) { subscription.cancel(); delegate.onError(new java.io.IOException("Security response too large")); }
            else delegate.onNext(buffers);
        }
        public void onError(Throwable error) { delegate.onError(error); }
        public void onComplete() { delegate.onComplete(); }
    }
    public static void validateWorkspace(String value) { if(value==null || !value.matches("[A-Za-z0-9_-]{1,64}")) throw new AuthException(401,"Workspace is required"); }
    public static String resourceId(String workspaceId,String entityId) {
        validateWorkspace(workspaceId);
        if(entityId==null || entityId.isBlank() || entityId.length()>256 || entityId.chars().anyMatch(Character::isISOControl)) throw new AuthException(403,"Valid entity identifier required");
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest((workspaceId+'\0'+entityId).getBytes(StandardCharsets.UTF_8))); }
        catch(java.security.NoSuchAlgorithmException e) { throw new IllegalStateException(e); }
    }
}
