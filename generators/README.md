# Authenticated load generators

Python 3.11+ standard library; generated images/video additionally require FFmpeg with libx264. Every upload requires a verified JWT, workspace, and explicitly selected pre-provisioned entity. Generators never create SpiceDB grants. Workspace membership alone does not permit ingestion writes.

For the local development fixture, obtain a five-minute producer token without printing credentials:

```sh
python3 generators/login.py --url http://localhost:18088 --username producer \
  --token-file .runtime/security/producer-token.json
```

The login helper reads the private `.runtime/security/credentials.json`, uses the dashboard's `/auth/token` proxy, and writes the token response with mode 0600. Production deployments use their own OIDC provider. For direct HTTPS issuer access, specify `--url https://localhost:18444 --path /token --ca .runtime/security/ca.crt`.

```sh
python3 generators/load.py json --token-file .runtime/security/producer-token.json \
  --workspace demo --entity alpha --entity shared --related-to shared \
  --rate 50 --concurrency 4 --duration 30 --schema-drift --out-of-order-seconds 3
python3 generators/load.py images --token-file .runtime/security/producer-token.json \
  --workspace demo --entity alpha --rate 2 --duration 15
python3 generators/load.py video --token-file .runtime/security/producer-token.json \
  --workspace demo --entity alpha --rate 0.05 --concurrency 1 --duration 1 \
  --stream-seconds 30 --gop 97
```

`--entity` can repeat to cycle known IDs. For large provisioned datasets, use explicit `--entity-prefix sensor- --entities 100` to generate sensor-0000 through sensor-0099; all must already have workspace binding and write grants. `--related-to` adds a chosen graph target; graph query visibility requires permission to both endpoints. Omit it to avoid generating edges.

`--token-file` accepts a raw JWT or JSON containing access_token and is reread before every new request, so an external login refresh can atomically replace the file. `--token` is also supported, but places the token in process arguments. Tokens expire after five minutes in the development issuer. An existing video upload cannot replace its identity midstream: keep each stream within token lifetime, refresh the file, and open a new stream. Expiry and revocation stop ingestion rather than silently retrying with different credentials.

`--url` defaults to the loopback dashboard proxy; `--path` overrides an endpoint. HTTPS verifies the certificate using `--ca` or system trust. Plain HTTP is accepted for loopback development; remote cleartext requires explicit `--allow-http`. This changes transport only and never disables authentication.

The rate counts total request starts, limited by concurrency; video rate counts streams, not frames. Duration bounds scheduling while in-flight uploads finish afterward. The generator validates final NDJSON video completion, not only the initial HTTP 200. Summary output includes successes, failures, throughput, latency, and up to ten errors; tokens are not logged and no automatic retries hide overload or authorization failures.

Schema-drift, invalid-ratio, timestamp disorder, source image/video files, FPS/GOP/keyframe controls, and accelerated `--no-realtime` video remain supported. Invalid records count as failures. Run `python3 -m unittest discover -s generators` for header propagation and real FFmpeg streaming checks.
