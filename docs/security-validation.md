# Authorization validation

Historical validation of the pre-schema runtime. Its graph-specific results do not apply to the current storage contract. Run `scripts/schema-smoke.py` for the native VARIANT round trip; the updated broader security suite requires a fresh run.

The secure deployment runs in Kubernetes context `docker-desktop`, namespace `context-graph`. The previous unlabeled Flink deployment is suspended and its storage retained. Secure records use new Kafka topics and the `context_secure` Iceberg namespace.

Verified against running services (not mocked authorization):

- Separate Kafka service credentials: query publishing to an ingestion topic is rejected; ingestion consuming processed metrics is rejected (`scripts/test-security-acls.py`).
- API permission-check credential: can check access through the TLS check proxy; cannot write relationships/schema, bypass its exact route, or authenticate to the administrative SpiceDB endpoint (`scripts/test-check-proxy.py`). Anonymous checks are rejected.
- Chrome dashboard: sign-in succeeds; media cookie is Secure, HttpOnly and SameSite Strict. Sign-out deletes that cookie and clears displayed metrics. No JavaScript errors in this browser check.
- Anonymous requests, workspace crossing, missing write grants and forged security labels are rejected by ingestion.
- Media authorization covers images, byte ranges, playlists and video segments. Real grant revocation stops read access and an active video upload.

Automated integration results and infrastructure enforcement checks are recorded below as they complete. Credentials and tokens are excluded from reports and images; generated development passwords are only in `.runtime/security/credentials.json` (mode 0600).

This local environment shares one physical storage node. It is not tolerant of that node failing. The bundled authorization datastore is a single PostgreSQL instance: restarting it causes authorization to fail closed until it recovers. A production availability deployment needs a highly available PostgreSQL service and replicated/object storage; replica counts on stateless APIs do not remove those dependencies.

## Complete real-service smoke

`scripts/security-smoke.py` initially passed in **70.0 seconds**; the final repeat after media hardening passed in **76.6 seconds**. The sanitized [result artifact](security-smoke.json) records the final assertions. It used real signed JWTs, SpiceDB, authenticated Kafka, Flink, Iceberg, DuckDB and the GraphQL HTTP/WebSocket APIs.

- Pre-aggregation authorization produced Alice **65**, Bob **605**, administrator **665**, demonstrating that hidden entity contributions do not enter a caller's aggregate.
- Three video segments passed keyframe checks; denied users could not retrieve their playlist or chunks. Revoking the producer's write grant stopped an active upload.
- Active GraphQL subscription revocation terminated the operation. A genuinely signed short-lived token closed the live WebSocket with code **4401** on expiry; expired HTTP and media sessions were also denied.
- Registered generated fields, both endpoints of graph edges, cross-workspace boundaries, default workspace visibility and restricted-entity toggling passed.

The authenticated [dashboard screenshot](dashboard-secure.png) was captured after the browser successfully connected to live data. Browser sign-out cleared its session and displayed metrics.

## Network enforcement

Docker Desktop's original networking accepted NetworkPolicy resources but failed an actual isolation probe. A pinned kube-router **v2.11.1** controller was installed in firewall-only mode, preserving the existing routing, CNI configuration and service proxy. The same probe then passed. The deployment script requires this probe to pass before deploying the application. The supported selective-controller behavior is described in the [kube-router user guide](https://www.kube-router.io/docs/user-guide/).

After the local VM was restarted to increase memory, network isolation and service-boundary probes passed again. Flink restored and completed five checkpoints with zero failures. Kubernetes reported approximately 11.4 GiB node memory. Query now reserves a 1 GiB container limit with a 50% JVM heap ceiling, leaving room for its bounded DuckDB native allocations; its TLS health probes allow five seconds instead of one.

## Authorization dependency outage

The final controlled outage test passed with network policies enforced and the adjusted query memory/probes. The permission proxy was directed at an unavailable authorization backend, and the test waited until every old proxy pod was deleted before making assertions. Authenticated GraphQL, ingestion and media requests each returned **503**. The normal backend was restored in a `finally` block, and authorized query/media reads returned **200** again. Neither query replica restarted during this final run. [Sanitized outage evidence](security-outage-smoke.json).

Earlier attempts overlapped rollouts and the VM/resource transition and were not counted as passes. The final secure-v4 rendered deployment also passed Kubernetes server-side validation; completed provisioning Jobs were validated as new Job instances because their templates are immutable.

The final browser check also played protected HLS video after signing in, then confirmed that sign-out removed the media cookie and cleared displayed metrics. The updated screenshot uses local/system fonts and requires no external font requests.

## Authenticated mixed-media load

With mandatory authorization and network policies enabled, a 20-second run completed **973/973 JSON requests** (48.442 requests/second, 192 ms mean request latency), **10/10 images**, and **1/1 ten-second video stream**, with no upload failures. Fifteen concurrent authorized GraphQL samples had no errors; their p95 latency was approximately **2.014 seconds**. This is a short local measurement, not a maximum-capacity or latency guarantee. The configured JSON target was 50/second; achieved throughput was lower. The earlier eight-client run achieved 43.75/second without failures. Both runs are retained in `docs/evidence/secure-load-*.json`.

During the final controlled authorization outage, an independent sampler completed **60/60** verified HTTPS liveness requests: p95 **15.6 ms**, maximum **23.5 ms**, with zero failures. A thread dump showed the Vert.x event loops idle in their selectors rather than performing blocking authorization work. The original one-second probe failures' exact cause was not established retrospectively. [Observer results](evidence/query-auth-outage-observer.json).

## API rolling deployments

The first secured rolling test delivered **900/900 writes**, but one authorized query received a **502** while the old listener was being removed. Nginx recorded connection refusal, not a truncated response. This run is retained as failed availability evidence in `docs/evidence/secure-roll-*-initial.json`.

The final configuration adds a five-second API pre-stop propagation delay. Query secure-v5 also retains its HTTP server and drains accepted requests for up to forty seconds before stopping workers and Vert.x; its Kubernetes termination grace is ninety seconds. No broad POST retry was added. A focused delayed HTTP/1.1 response regression test passed; the query suite recorded thirteen passes and one optional native-extension test skip. The live rolling repeat is recorded below.

The identical ninety-second rolling repeat passed after the fixes: **900/900 JSON writes and 85/85 authorized queries succeeded**, with no client failures. Both APIs returned to two Ready replicas after approximately 34.4 seconds. Query p95 during replacement was approximately 1.927 seconds; maximum 4.448 seconds. This verifies observed availability for the tested API rolling scenario, not zero downtime for the single-node PostgreSQL/storage profile. [Rolling summary](evidence/secure-roll-summary.json), [write results](evidence/secure-roll-json.json).

## Media reference isolation

The final ingestion image is secure-v6. FFmpeg input is restricted to the `pipe` protocol, and images must have self-contained PNG/JPEG signatures before ImageIO parsing. Eight ingestion regression tests passed, including forced-HLS attempts to read local media and contact a network listener, alongside normal image and keyframe generation checks. The final real-service smoke also rejected uploaded playlists referencing another restricted entity’s local video or a separate network URL, and rejected an image playlist. Normal protected images/video, aggregates, active revocation and expiry passed in the same run. [Regression evidence](evidence/media-reference-regressions.json).
