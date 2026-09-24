# Earlier functional validation record

These measurements preceded mandatory OIDC/SpiceDB and Kafka ACL enforcement. They establish functional pipeline evidence, not authorization correctness or current secure-mode capacity. See [security validation](security-validation.md) for secure deployment results.

Run on 2026-09-24, macOS ARM64 / Java21, Docker Desktop Kubernetes1.32.2.

## Automated tests

- Ingestion: schema rejection and real FFmpeg conversion from irregular source GOP; every generated segment's first frame is a keyframe.
- Processor: Flink2.3 MiniCluster temporal windows, full execution graph, actual Iceberg Parquet commit. Broker integration against Kafka4.1.1 validates committed transactional output, error routing, canonical savepoint and restore without replay duplicates in the checked Iceberg records.
- Query: real DuckDB iceberg/cache_httpfs extension installation/loading, parameterized hostile-value binding, canonical UTC timestamps, committed metadata discovery, executable configured GraphQL schema, topic/entity filtering including versioned edge topics.
- Dashboard: replay-window identity and bounded history, same-origin media paths, secure WebSocket URL selection.
- Generators: real concurrent FFmpeg streaming against a server returning an early NDJSON response, plus admission/error cases.
- Deployment renderer: isolated query config changes leave ingestion and processor ConfigMap hashes unchanged; nested schema paths resolve. Versioned deployment tests check distinct groups, transaction prefixes, topics, warehouse namespaces and query selectors.

## Native complete pipeline

A 24.94-second full smoke passed. Three JSON samples produced count3, sum60, mean20 in Iceberg and Kafka GraphQL subscription. Provenance nodes/edges were queried. Image bytes matched exactly. Three fetched video segments each began with a keyframe and decoded independently; live video metadata and committed media history were observed.

## Bounded mixed-media load

Concurrent 30-second run: 3,000 JSON events at100requests/s,60images at2requests/s, one20second video. All succeeded.52 concurrent GraphQL queries had zero errors, mean90.87ms, p95 123.91ms, max188.08ms. These are local measurements, not production capacity guarantees.

An initial run deliberately exceeded the configured8 concurrent upload slots: one JSON request and two image requests received503 admission rejections. Reducing client concurrency to match the service bound eliminated failures. This demonstrates bounded admission, not unlimited scalability.

## Browser

Chrome via Playwright: HTTP history, live WebSocket connection, entity filtering and HLS playback verified. Video readyState4,640px decoded width, currentTime advancing. No uncaught JavaScript errors.390px viewport had390px document width (no horizontal overflow). Screenshots under `.runtime/` are disposable validation artifacts.

## Kubernetes

Three Kafka brokers ready with replication3/minimum ISR2. Both ingestion/query/dashboard replicas ready. The Flink2.3 job ran10tasks, committed Iceberg data and completed durable filesystem checkpoints. A real operator savepoint resource upgrade restored a new job; completed checkpoint7 in805ms with0failures. Operator logs identify released1.16.1 revision1c895a3.

Kafka→GraphQL modern WebSocket routing, cancellation, ping/pong and operation ID reuse passed before and after a real query rolling deployment. Old/new ready endpoints overlapped; an in-cluster dashboard request remained successful. Both rebuilt query replicas loaded native DuckDB extensions and discovered all four Iceberg schemas.

The initial full Kubernetes smoke deliberately overlapped a job upgrade and exposed a real cross-partition watermark bug: the three expected samples reached raw Iceberg history but were routed late, so that run failed its metric check. Watermarks had been assigned after partitions merged. The fix moves schema-safe timestamp/watermark generation into the Kafka source per partition. A two-partition replay regression (newer partition first) and savepoint tests pass. This failed-run evidence is retained under `.runtime/k8s-smoke-initial.*`; the final fixed deployment is retested separately.

## Final full Kubernetes smoke

After the partition-watermark fix and actual savepoint restoration, the entire pipeline passed through the dashboard nginx proxy in **22.64 seconds**. Count 3, sum 60, mean 20; historical nodes/edges/image/video; exact image bytes; three independently decodable keyframe-aligned video segments; and both live metric/video GraphQL subscriptions all passed. Evidence: `.runtime/k8s-smoke.json` and `.runtime/k8s-smoke.log`. Flink was RUNNING/STABLE with successful checkpoints and zero observed checkpoint failures.

## Broker rolling deployment

A 99-second in-cluster sampler covered sequential replacement of all three Kafka brokers.85/85 ingestion requests returned 202 with broker-confirmed eventIDs;85/85 GraphQL schema requests returned 200. No client retries or failures. Producer-internal retries were not instrumented. Ingest p95 was 66.6 ms, but max reached 11.893 s during failover; query p95 was 8.98 ms. Afterward all three brokers were ready and all six cg.events partitions had three in-sync replicas with minISR 2. Details: `.runtime/kafka-rollout-summary.json`.

One operator replica exceeded its initial 512 MiB limit during rollout testing; its peer kept serving and Flink kept running. The final operator configuration raises the limit to 768 MiB and explicitly bounds the Java heap at 256 MiB and active processors at two.

Query requests were reduced to 320 MiB with 768 MiB runtime limits to leave room for the local rolling surge. A full independent Flink candidate requires additional capacity beyond this small local cluster.

## Limits of the evidence

No node-loss storage HA, long-duration soak, large-scale partition rescaling, arbitrary state migration, WAN playback latency guarantee, or cross-system atomic commit guarantee is established. Savepoint job upgrades pause computation. Versioned warm-up and query cutover artifacts are generated/tested but an arbitrary application migration requires its own readiness comparison. Local POSIX storage must remain available to every worker at the same path.
