# Mandatory shared security

`io.contextgraph:security:0.1.0` verifies RS256 OIDC JWT signatures with JWKS and checks exact issuer, audience, expiry, not-before, subject and key ID. Verification rejects algorithm substitution and unrecognized critical headers. Keys cache for five minutes; unknown key IDs trigger a throttled refresh. Authorization decisions do not cache: every workspace/entity check uses fully consistent SpiceDB `/v1/permissions/check`. Denied and conditional results never grant access; transport failures and unrecognized responses fail closed with 503.

Required environment: `OIDC_ISSUER`, `OIDC_AUDIENCE`, `OIDC_JWKS_URL`, `SPICEDB_ENDPOINT`, `SPICEDB_TOKEN`. Endpoints require HTTPS. Local HTTP testing requires explicit `SECURITY_ALLOW_HTTP=true`; this does not disable authentication or authorization. JVM TLS trust follows its configured truststore. No code path accepts unsigned identities or trusted caller user headers.

`KafkaSecurity.properties()` requires `KAFKA_CLIENT_CONFIG`, a mounted secret properties file. It rejects plaintext Kafka, absent SASL credentials and disabled hostname verification. Supported SASL mechanisms are SCRAM-SHA-256, SCRAM-SHA-512 and PLAIN over verified TLS. Keep service principals and topic/group ACLs separate. The shared library does not grant or provision any permissions.

Trusted operators must enforce one workspace relationship per entity. Resource IDs use lowercase SHA256(workspace + NUL + entity). Call network methods off event loops, and recheck authorization before delivering protected results. Authorization cannot retract bytes already delivered before revocation.

In the Kubernetes deployment, `SPICEDB_ENDPOINT` points to the TLS check-only proxy (`https://spicedb-checks:8443`) and `SPICEDB_TOKEN` is its separate read-only credential. APIs must never receive SpiceDB's administrative preshared key. See `services/check-proxy/README.md`.

Workspace membership grants view permission on ordinary entities. Trusted operators can mark an entity restricted with `entity:<resourceId>#restricted@user:*`; restricted entities require explicit reader/writer permission or workspace administrator access. Membership alone never grants ingestion permission, and missing entity-to-workspace relationships deny access.
