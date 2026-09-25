package io.contextgraph.security;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nimbusds.jose.*;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.*;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jwt.*;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import java.net.*;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.*;
import static org.junit.jupiter.api.Assertions.*;

class AccessControlTest {
    HttpServer server; RSAKey key; AccessControl access; String base;
    AtomicReference<String> permission=new AtomicReference<>("PERMISSIONSHIP_HAS_PERMISSION");
    AtomicInteger status=new AtomicInteger(200), calls=new AtomicInteger();
    AtomicBoolean stall=new AtomicBoolean();
    java.util.concurrent.ExecutorService executor=java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor();
    List<String> requests=Collections.synchronizedList(new ArrayList<>());
    @TempDir Path temp;
    @BeforeEach void setup() throws Exception {
        key=new RSAKeyGenerator(2048).keyID("test-key").generate();
        server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0); server.setExecutor(executor); base="http://127.0.0.1:"+server.getAddress().getPort();
        server.createContext("/jwks",exchange -> {
            byte[] body=new JWKSet(key.toPublicJWK()).toString().getBytes(); exchange.sendResponseHeaders(200,body.length); exchange.getResponseBody().write(body); exchange.close();
        });
        server.createContext("/v1/permissions/check",exchange -> {
            calls.incrementAndGet(); requests.add(new String(exchange.getRequestBody().readAllBytes()));
            if(stall.get()) { exchange.sendResponseHeaders(200,100); exchange.getResponseBody().write('{'); exchange.getResponseBody().flush(); try { Thread.sleep(10000); } catch(InterruptedException e) { Thread.currentThread().interrupt(); } exchange.close(); return; }
            byte[] body=("{\"permissionship\":\""+permission.get()+"\"}").getBytes(); exchange.sendResponseHeaders(status.get(),body.length); exchange.getResponseBody().write(body); exchange.close();
        });
        server.start(); access=new AccessControl(Map.of("OIDC_ISSUER",base,"OIDC_AUDIENCE","context-graph","OIDC_JWKS_URL",base+"/jwks","SPICEDB_ENDPOINT",base,"SPICEDB_TOKEN","test-service-secret","SECURITY_ALLOW_HTTP","true"));
    }
    @AfterEach void cleanup() { server.stop(0); executor.shutdownNow(); }
    String token(RSAKey signer,String issuer,String audience,long expiration,long notBefore) throws Exception {
        var claims=new JWTClaimsSet.Builder().issuer(issuer).audience(audience).subject("alice").expirationTime(Date.from(Instant.ofEpochSecond(expiration))).notBeforeTime(Date.from(Instant.ofEpochSecond(notBefore))).build();
        var jwt=new SignedJWT(new JWSHeader.Builder(JWSAlgorithm.RS256).keyID("test-key").build(),claims); jwt.sign(new RSASSASigner(signer)); return "Bearer "+jwt.serialize();
    }
    String validToken() throws Exception { long now=Instant.now().getEpochSecond(); return token(key,base,"context-graph",now+60,now-1); }
    @Test void delegatedReadScopeIsWorkspaceBoundAndCannotWriteOrManage() throws Exception {
        var claims=new JWTClaimsSet.Builder().issuer(base).audience("context-graph").subject("alice")
            .expirationTime(Date.from(Instant.now().plusSeconds(60))).claim("workspace","workspace-a").claim("scope","context:read").build();
        var jwt=new SignedJWT(new JWSHeader.Builder(JWSAlgorithm.RS256).keyID("test-key").build(),claims);
        jwt.sign(new RSASSASigner(key)); String authorization="Bearer "+jwt.serialize();
        var context=access.authenticate(authorization,"workspace-a");
        assertFalse(context.writeAllowed());
        access.require(context,AccessControl.resourceId("workspace-a","record"),"view");
        assertEquals(403,assertThrows(AuthException.class,()->access.authenticate(authorization,"workspace-b")).status());
        assertEquals(403,assertThrows(AuthException.class,()->access.require(context,AccessControl.resourceId("workspace-a","record"),"ingest")).status());
        assertEquals(403,assertThrows(AuthException.class,()->access.requireWorkspace(context,"manage")).status());
    }
    @Test void signatureIssuerAudienceExpiryAndNotBeforeCannotBeForged() throws Exception {
        long now=Instant.now().getEpochSecond();
        List<String> bad=List.of(token(new RSAKeyGenerator(2048).generate(),base,"context-graph",now+60,now-1),token(key,"https://wrong-issuer","context-graph",now+60,now-1),token(key,base,"other-audience",now+60,now-1),token(key,base,"context-graph",now-1,now-2),token(key,base,"context-graph",now+60,now+30),"Bearer not-a-jwt");
        for(String authorization:bad) assertEquals(401,assertThrows(AuthException.class,()->access.authenticate(authorization,"workspace-a")).status());
        assertEquals(0,calls.get(),"Invalid identity must not reach permission checking");
    }
    @Test void workspaceAndResourceAreAlwaysFullyConsistentAndRevocationHasNoCache() throws Exception {
        var context=access.authenticate(validToken(),"workspace-a"); String resource=AccessControl.resourceId("workspace-a","sensor-a");
        access.require(context,resource,"ingest"); int before=calls.get();
        permission.set("PERMISSIONSHIP_NO_PERMISSION");
        assertEquals(403,assertThrows(AuthException.class,()->access.require(context,resource,"view")).status());
        assertTrue(calls.get()>before);
        for(String request:requests) assertTrue(new ObjectMapper().readTree(request).path("consistency").path("fully_consistent").asBoolean());
    }
    @Test void conditionalUnknownOutageAndTokenExpiryFailClosed() throws Exception {
        var context=access.authenticate(validToken(),"workspace-a");
        permission.set("PERMISSIONSHIP_CONDITIONAL_PERMISSION"); assertEquals(403,assertThrows(AuthException.class,()->access.requireWorkspace(context,"access")).status());
        permission.set("UNRECOGNIZED"); assertEquals(503,assertThrows(AuthException.class,()->access.requireWorkspace(context,"access")).status());
        status.set(503); assertEquals(503,assertThrows(AuthException.class,()->access.requireWorkspace(context,"access")).status());
        var expired=new SecurityContext("alice","workspace-a",Instant.now().getEpochSecond()-1);
        assertEquals(401,assertThrows(AuthException.class,()->access.allowed(expired,AccessControl.resourceId("workspace-a","e"),"view")).status());
        server.stop(0); assertEquals(503,assertThrows(AuthException.class,()->access.requireWorkspace(context,"access")).status());
    }
    @Test void stalledAuthorizationResponseBodyHasHardDeadline() throws Exception {
        var context=access.authenticate(validToken(),"workspace-a"); stall.set(true); long start=System.nanoTime();
        assertEquals(503,assertThrows(AuthException.class,()->access.requireWorkspace(context,"access")).status());
        assertTrue(java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime()-start)<6500);
    }
    @Test void canonicalIdsIsolateWorkspaceAndValidateInputs() {
        assertNotEquals(AccessControl.resourceId("a","bc"),AccessControl.resourceId("ab","c"));
        assertNotEquals(AccessControl.resourceId("a","sensor"),AccessControl.resourceId("b","sensor"));
        assertThrows(AuthException.class,()->AccessControl.resourceId("../workspace","sensor"));
        assertThrows(AuthException.class,()->AccessControl.resourceId("workspace",""));
    }
    @Test void kafkaPlaintextAndDisabledVerificationCannotStart() throws Exception {
        Path file=temp.resolve("credentials.properties");
        Files.writeString(file,"security.protocol=PLAINTEXT\n"); assertThrows(IllegalStateException.class,()->KafkaSecurity.properties(file));
        Files.writeString(file,"security.protocol=SASL_SSL\nsasl.mechanism=PLAIN\nsasl.jaas.config=test\nssl.endpoint.identification.algorithm=\n"); assertThrows(IllegalStateException.class,()->KafkaSecurity.properties(file));
        Files.writeString(file,"security.protocol=SASL_SSL\nsasl.mechanism=SCRAM-SHA-512\nsasl.jaas.config=test\n"); assertEquals("https",KafkaSecurity.properties(file).getProperty("ssl.endpoint.identification.algorithm"));
    }
}
