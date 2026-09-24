# Reference application: permissioned workplace search

This application and its Slack/GitHub ingestion adapters are examples built on the general-purpose Context Graph platform. Other data sources and products integrate through the same ingestion and serving contracts.

The search reference application is routed at `/search` through the dashboard forward on port `18088`, with its own React/shadcn frontend and search API. The read-only control frontend uses a separate forward on port `18089`. Use the existing local admin login. Tokens stay in browser memory. To recreate the forwards:

```sh
kubectl --context docker-desktop -n context-graph port-forward svc/dashboard 18088:8080
kubectl --context docker-desktop -n context-graph port-forward svc/control-ui 18089:8080
```

## Live ingestion

`config/connectors/sources.yaml` limits GitHub to **MariHQ/mari**: issues, pull requests, their comments/review comments, the repository README, and default-branch commit history. Commit documents contain commit messages, not file diffs. Slack imports readable channel histories and replies. The first completed backfill indexed 472 GitHub documents and 49 Slack messages (47 in `test`, 2 in `private-test`). The bot is not a member of `all-mari`, `social`, `new-channel`, or `support`; these sources report `not_in_channel` and require a channel member to invite the bot before they can be indexed.

Temporal schedules content sweeps every 15 minutes and permission refreshes every 3 minutes. Each source has a retryable child workflow. Full pagination, stable IDs and fingerprints make replay safe; unchanged documents are skipped. A complete snapshot emits tombstones for removed documents. Interrupted scans replay from the beginning and never infer deletion from an incomplete page. HTTP acknowledgement and the connector state database are not one transaction: Kafka may contain replay duplicates, which the current-document projection resolves. Workers reconcile existing schedule intervals and actions on startup while preserving paused state; roll workers after changing schedule configuration.

The path is Temporal workers → authenticated Vert.x `/ingest/github` and `/ingest/slack` → JSON Schema validation → separate secure Kafka topics → the existing checkpointed Flink job → Iceberg/Polaris on RustFS. Provider failures remain in Temporal retries and source status; invalid ingestion records use the existing ingestion error path. PostgreSQL stores connector identities, fingerprints and tombstone metadata; document bodies are stored through the event pipeline.

## Authorization

Each repository/channel has an imported entity with source-native SpiceDB grants. Imported entities disable local workspace-admin/reader/writer visibility bypasses: viewing requires both workspace access and a current source grant. Public GitHub repositories are readable by knowledge-workspace members. Private GitHub access is conservatively limited to the verified token owner and collaborators the API can enumerate. Slack grants are conservatively limited to active human channel members, including for public channels.

Provider identities use stable provider IDs, with SHA-256 encoded SpiceDB object IDs. The local `admin` login explicitly maps to Slack **daniel** and the authenticated GitHub owner. No email/name matching guesses and no generated login passwords for scraped users. Source identity links in configuration are trusted operator configuration.

Source read grants expire after ten minutes using SpiceDB server-side relationship expiration. Remote membership changes are detected by the three-minute polling schedule; failed refreshes attempt immediate revocation, and unavailable authorization infrastructure cannot renew leases. This is bounded polling, not instantaneous remote revocation. Content edits/deletions have the content sweep's polling delay.

Only the trusted connector may mutate source ACLs. It currently holds the SpiceDB administrative credential behind network policy; a narrowly scoped relationship mutation broker would be needed to reduce that trust further. User-facing APIs cannot mutate grants or receive storage credentials. Polaris credential vending remains internal to the serving layer.

Before lexical indexing, the serving API materializes only authorized Iceberg rows. It chooses the latest document version and removes tombstones, builds a private in-memory DuckDB FTS index, ranks results, then checks corpus permissions again. There is no shared FTS index containing other users' content. The configured `documents(ids)` query rechecks source revisions and access before each model call, before streaming each complete cited paragraph, and before finishing the answer. Source tokens are buffered until a complete paragraph and its citation IDs can be validated. SSE reports real retrieval progress and generated paragraphs; no simulated typing or internal reasoning traces are exposed. Streaming errors clear the current answer. Follow-ups send only prior user questions as context and retrieve current authorized evidence again. Revoked or changed model inputs invalidate the answer. Citation IDs must identify retrieved documents; URLs come from verified source records.

## Ranking and answers

Ranking is configured in `config/queries.yaml`:

```
score = BM25 × (1 + 0.35 × freshness) × (1 + title boost) × type weight
freshness = 2 ^ (-ageDays / halfLifeDays)
```

Half-lives are 14 days for Slack messages, 45 for issues/PRs and 90 for commits. Exact query phrases in titles receive a 0.20 boost. Type weights are PR 1.15, issue 1.10, message 1.0 and commit 0.80. Every lexical match is rescored before selecting the top results. Invalid or implausibly future dates receive no freshness boost. Source/date filters and newest-first sorting are supported. This remains lexical retrieval; no embeddings are used.

The default UI is a conversation, with live activity, inline citations, a source side panel and follow-up questions. Document search is available separately. DeepSeek Flash plans up to three additional keyword searches and returns short cited statements. Initial retrieval takes results from each permitted provider so short Slack messages cannot crowd out all repository evidence. Only authorized evidence is sent to DeepSeek. The tool loop cannot execute arbitrary actions or follow URLs. Source content is treated as untrusted data. Citation validation checks provenance and access, not whether every model interpretation is semantically correct. The integration uses JSON output and explicitly disables thinking for predictable response latency, following the [DeepSeek API documentation](https://api-docs.deepseek.com/api/create-chat-completion/).

## Operations and limits

The control plane shows connector counts, sync status and errors. Prometheus scrapes connector, search, Temporal and frontend metrics alongside the existing pipeline. Content and credentials are not metric labels or request logs. Secrets are generated into ignored `.runtime/security/` files and Kubernetes Secrets; credentials are not in workflow histories.

The local Temporal server uses durable SQLite on a PVC behind an mTLS proxy. It is a **development server, not a highly available production Temporal deployment**. Worker/API/frontend replicas support rolling updates; the single-node storage/database/Temporal services remain availability limits. The FTS index is rebuilt per request and bounded at 10,000 current documents / 16 MiB text, with separate serving-layer row limits. Larger deployments need a partitioned search index with equivalent authorization guarantees.

Build with `scripts/build.sh`; provision existing private credentials using `scripts/provision-connectors.py`, then deploy the staged search release with `scripts/deploy-search.py`. Source credentials are a prerequisite and are not bundled with the repository.

Validation includes connector pagination/identity/ACL tests, search revision/revocation/citation tests, DuckDB ranking/tombstone tests, live imported-source lease expiry, real API searches and a DeepSeek answer, desktop/mobile browser checks, and the control-plane authorization/metrics smoke test. See `docs/evidence/source-permissions.json`, `docs/evidence/control-plane.json` and `docs/search.png`.

The interaction design follows [Glean AI Answers](https://docs.glean.com/user-guide/assistant/ai-answers) and [Glean information access](https://docs.glean.com/user-guide/assistant/how-glean-accesses-info): permissioned company knowledge, cited answers and conversational follow-ups. This implementation remains BM25 with recency weighting, not Glean’s complete hybrid retrieval platform. [Brainless](https://github.com/theswerd/brainless) was reviewed; its terminal-agent components were not needed for this workplace interface.
