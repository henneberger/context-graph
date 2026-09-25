# Application releases and recovery

Use the application harness to compile, inspect, render, deploy, and promote schema/SQL bundles. Jobs run as ordinary Kubernetes resources; there is no Flink operator.

```sh
python scripts/cg build path/to/bundle.yaml --output release-directory
python scripts/cg inspect release-directory
python scripts/cg render release-directory --node YOUR_NODE
python scripts/cg deploy release-directory --context YOUR_CONTEXT --node YOUR_NODE
python scripts/cg status release-directory --context YOUR_CONTEXT
python scripts/cg promote release-directory --context YOUR_CONTEXT
```

The release workflow verifies running jobs, a completed checkpoint, and API rollouts before switching stable Service routes. Those checks establish workload health, not data continuity. Separate releases have separate topics/tables and do not automatically share historical records or processing state. Repointing a Service does not recover writes made only to a different release.

For a compatible existing job, retained checkpoints and native savepoints provide recovery state. Stateful upgrades must explicitly account for source positions, schema compatibility, operator state, and output semantics. API readiness alone is not a catch-up or equivalence check.

For local development, the [runtime reset command](lakehouse.md#configuration-and-operations) purges application data and starts jobs with fresh recovery state. Use checkpoints or savepoints when preserving processing state is required.
