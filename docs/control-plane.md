# Read-only control plane

Open **http://localhost:18089/** through a separate `kubectl --context docker-desktop -n context-graph port-forward svc/control-ui 18089:8080` forward. The gear link on the data dashboard opens this view. The local `admin` identity has the separate platform grant; ordinary workspace members do not. Tokens stay in browser memory and expire after five minutes with the local issuer. Sign in again after expiry.

The control service uses its namespace-scoped Kubernetes service account to read Deployments, StatefulSets, Pods, Services, FlinkDeployments and ConfigMaps. It returns selected operational fields and the API, job and query definitions referenced by deployed workloads. It does not read Secrets, logs, pod execution endpoints, arbitrary URLs or arbitrary PromQL. SQL is displayed, not executed. Suspended jobs and scaled-down workloads remain visible.

Every inventory request validates the OIDC signature, issuer, audience and expiry, then checks `workspace:platform#manage` in SpiceDB with fully consistent reads. It checks again before returning the collected result. The service uses the check-only proxy credential. Platform access is independent of normal workspace access because namespace inventory and system metrics cover all workspaces. Configure `OIDC_ISSUER`, `OIDC_AUDIENCE`, `OIDC_JWKS_URL` and `PLATFORM_WORKSPACE` in the control Deployment for a production identity provider. Local username/password login is provided by the existing development issuer; a production browser OIDC flow remains separate work.

Platform access can be provisioned or revoked through the trusted operator tooling:

```python
# Run with the private operator credential and SPICEDB_ADMIN_ENDPOINT set.
import sys
sys.path.insert(0, 'scripts')
import permissions
permissions.write([permissions.relationship('workspace', 'platform', 'administrator', 'user', 'admin')])
# Pass delete=True to revoke. The control service cannot call this mutation API.
```

The control frontend is an independent Deployment and image, separate from the demo and search frontends. Its source ingestion table reads the connector status service.

The browser polls every 15 seconds and explicitly marks collection failures. Metrics charts cover the last 15 minutes. The inventory timestamp describes collection time, not a transactionally consistent snapshot across Kubernetes resources. RBAC only grants `get` and `list` in `context-graph`.

## Metrics

Prometheus discovers each annotated pod's named `metrics` port in `context-graph`; it scrapes each replica separately every 15 seconds. It retains seven days of metrics, capped at 4 GB, on a 5 GiB PVC. Raw Prometheus and exporter ports are reachable only by their allowed internal clients through NetworkPolicies. The dashboard exposes fixed, authorized metric summaries rather than a public Prometheus proxy. Do not publish Prometheus or exporter services directly.

| Component | Metrics source |
|---|---|
| Ingestion and query APIs | Dedicated internal port 9404: bounded HTTP status/route counters, latency histograms, JVM heap/threads/uptime; GraphQL error counter |
| Development issuer and check-only authorization proxy | Internal port 9404: HTTP status counters, response-time summaries, process uptime and memory |
| Temporal workers and search API | Prometheus client on 9404: provider/ingestion/search outcomes and latency |
| Temporal server | Internal metrics forwarded through the restricted proxy on 9404 |
| Control service | Prometheus client on 9404: status counters, request latency histogram, Python process metrics |
| Demo, control and search frontends | Nginx exporter on 9113 reading loopback-only `stub_status` |
| Kafka | JMX exporter 1.6.0 on 9404: broker traffic/errors, replica/controller state, heap and threads |
| Flink jobs | Bundled Flink 2.3 Prometheus reporter on 9249: records, operator/runtime and checkpoint metrics |
| Flink operator | Native Prometheus reporter on 9405 |
| Polaris | Native management endpoint `/q/metrics` on 8182 |
| SpiceDB | Native `/metrics` on 9090 |
| PostgreSQL | postgres_exporter on 9187, using a dedicated TLS `pg_monitor` identity with read-only transactions |
| RustFS | OTLP to OpenTelemetry Collector, then Prometheus scrape on 8889 |
| Monitoring services | Prometheus self metrics and Collector self metrics |

API metric labels contain route categories and status codes, never user IDs, entity IDs, payloads or bearer tokens. Native infrastructure metrics may include table/topic/bucket names, so all metric access is platform restricted. HTTP status metrics do not count GraphQL application errors as HTTP 500s; those also have `context_service_events_total{event="graphql_error"}`. Nginx stub status reports aggregate connections/requests, not per-route latency. API latency histograms measure completed responses; aborted requests are not included.

RustFS requires an OTLP collector rather than an S3 `/metrics` endpoint; see its [observability documentation](https://docs.rustfs.com/en/operations/observability). Flink uses its [Prometheus reporter](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/deployment/metric_reporters/). Exporter versions and image tags are pinned in the Kubernetes manifests and Dockerfiles.

Prometheus evaluates rules for failed scrape targets, API server errors and PostgreSQL availability. Rules are local only; no external notifications are configured. Prometheus and the Collector each have one replica in this local cluster. Their restart may interrupt monitoring; it does not stop data ingestion. Storage and PostgreSQL remain single-instance services, as documented in [lakehouse.md](lakehouse.md).

## Build, deploy and verify

`scripts/build.sh` builds the pinned application images (`observability-v1`, with control/dashboard at `observability-v2`) and Kafka exporter image. `scripts/deploy.sh` includes the control plane, monitoring resources and private TLS/monitoring-user provisioning. The deployment renderer preserves the explicitly pinned application tags. On an existing cluster, configure object-storage telemetry during a Flink savepoint pause because the single RustFS instance restarts. Kafka rolls one broker at a time.

```sh
.runtime/security-venv/bin/python scripts/control-smoke.py --context docker-desktop
.runtime/security-venv/bin/python -m unittest discover -s services/control -p 'test_*.py'
```

The smoke test checks denial of anonymous, forged and workspace-only identities, live platform revocation, rejection of write methods, deployed configuration visibility, healthy scrape targets, and actual metric-family presence. It temporarily revokes the local admin platform grant and restores it in a `finally` block. Run it against the development fixture, not an unrelated production administrator.

## Local verification

The deployed control plane passed browser checks at desktop and mobile widths, with no JavaScript errors or horizontal overflow. Platform admin login succeeded; a workspace member was denied; logout cleared inventory. The live smoke verified immediate platform-grant revocation, rejection of all write methods, actual Kubernetes RBAC denials, and 28 healthy Prometheus targets with metric families present for every major service (`docs/evidence/control-plane.json`).

The full data-plane security smoke passed after these deployments (`docs/evidence/security-smoke-observability.json`). Flink restored from an S3 savepoint and completed a new S3 checkpoint (`docs/evidence/metrics-flink-rollout.json`). Six control authorization/RBAC tests, two deployment renderer tests, and the existing Java security/query/ingestion tests passed. A credential scan found no local private credential values in repository files; `.runtime`, build output and dependencies are excluded from Git.
