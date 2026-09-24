# Versioned job and query upgrades

A normal `FlinkDeployment` savepoint upgrade stops the old job and restores the new job. Kafka retains input while processing pauses. For continuous output, run an isolated candidate beside the active job, warm its state and committed tables, then switch query routing. This requires capacity for both versions. The experimental operator blue/green resource is not used here: duplicating a deployment with identical Kafka transactions and Iceberg destinations would not provide isolation.

`version-job.py` renders concrete artifacts without applying them:

```sh
python3 -m pip install -r scripts/requirements.txt
python3 scripts/version-job.py --version v2 \
  --job-config config/jobs.yaml --query-config config/queries.yaml \
  --node YOUR_LOCAL_STORAGE_NODE --output .runtime/versions/v2
```

For a changed transformation, first copy the config directory to a staging directory, edit its jobs.yaml or queries.yaml, and pass those staged paths. Rendering embeds all configuration and JSON schemas in a dedicated ConfigMap. Editing the generated config directory afterward does not update the ConfigMap; render again into a new empty output directory. Reusing an existing nonempty output directory is rejected.

The generated release has:

- Flink deployment, consumer group, and transaction prefix `context-secure-v2`; separate checkpoint, savepoint, and HA paths.
- Iceberg namespace `context_secure_v2`, and output/error topics ending `.v2`. Input topics remain unchanged.
- Query table and processed-topic mappings updated together. Raw video subscriptions still use `cg.secure.video`.
- Candidate query deployment/service `query-v2`, with labels distinct from the active `query` service. Merely deploying the candidate cannot send it production traffic.
- `release.json` documenting all mappings, plus explicit cutover and rollback scripts.

The processor starts its new group at the earliest retained source offset. Do not restore the original job's savepoint into the candidate: it contains old sink/source state. Replaying the retained log builds candidate state independently. If Kafka retention no longer contains the required historical range, backfill that range into the candidate or retain the old historical tables behind additional query definitions before cutover. A new namespace does not automatically inherit old Iceberg history.

## Deploy and warm

Review the generated files and image tags. Use the same storage node as the base local profile; multi-node deployment needs shared durable storage. Then deploy explicitly:

```sh
kubectl apply -f .runtime/versions/v2/configmap.yaml
kubectl apply -f .runtime/versions/v2/topics.yaml
kubectl -n context-graph wait --for=condition=complete job/context-secure-v2-topics --timeout=180s
kubectl apply -f .runtime/versions/v2/flink.yaml
kubectl apply -f .runtime/versions/v2/query.yaml
kubectl -n context-graph rollout status deployment/query-v2 --timeout=180s
kubectl -n context-graph port-forward service/query-v2 18082:8081
```

In another terminal, inspect committed candidate windows:

```sh
curl --cacert .runtime/security/ca.crt -fsS https://localhost:18082/graphql \
  -H "Authorization: Bearer $OIDC_TOKEN" -H "X-Workspace-Id: $WORKSPACE_ID" -H 'content-type: application/json' \
  --data '{"query":"{metrics(limit:10){entity_id metric window_start window_end sample_count avg_value} schemas{name}}"}'
```

Use a verified OIDC token with workspace/entity permissions; anonymous requests fail closed. The candidate retains the same SpiceDB resource identity and mandatory row authorization. Compare these with the active query endpoint. Continue generating live input so event-time watermarks can advance. Before switching, verify successful recent Flink checkpoints, source-group lag caught up with the active job, candidate committed `window_end` values at least as recent as the active windows for representative entities, and expected values for the new transformation. If window sizes changed, compare equivalent time ranges rather than identical window IDs. Check node/edge counts and media queries too. Kubernetes readiness alone does not prove warmup: it verifies query connectivity/schema health, not Flink completeness or output equivalence.

## Switch and rollback

After that review, the generated script gates on candidate deployment readiness and records the original service selector before replacing it:

```sh
WARMUP_VERIFIED=v2 .runtime/versions/v2/cutover.sh
```

This changes the `query` Service selector to `app=query-v2,release=v2`, so the dashboard's unchanged query upstream selects candidate replicas for new connections. The script does not stop the original Flink job or query deployment. Existing HTTP keepalive connections and WebSockets can remain on old replicas until reconnect; clients should reconnect, reload history, and deduplicate live events. If enforcing a specific cutover instant matters, coordinate client reconnects after switching. A Service patch is not an atomic cross-client schema migration, and both query schemas must remain compatible during overlap.

To route new connections back to the saved original selector:

```sh
.runtime/versions/v2/rollback.sh
```

Keep the original job running throughout the rollback window so its state and outputs remain current. After candidate validation and the rollback window, drain old query replicas and retire the original job through the operator's savepoint workflow. Retain the savepoint, old table snapshots, and topic data according to your retention policy. Do not immediately delete rollback artifacts or reuse transaction prefixes for unrelated jobs.

Run renderer checks with `python3 -m unittest discover -s scripts -p test_version_job.py`. These tests validate configuration and manifest isolation; they do not substitute for a real dual-job warmup and cutover test on Kubernetes.

The current Docker Desktop node has roughly 11.4 GiB allocatable memory. Query requests are 512 MiB per replica with a 1 GiB container limit. JAVA_TOOL_OPTIONS sets the heap to 50% of that limit; four DuckDB workers each have a 64 MiB memory limit, leaving headroom for native/Netty overhead. The image entrypoint does not override those JVM options. Requests reserve scheduling capacity and do not cap actual usage. Check allocatable/requested resources before running a second Flink version or starting a rolling surge; availability depends on capacity to schedule and run both old and replacement workloads.
