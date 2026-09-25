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

The JSON Schema/VARIANT replacement is a clean reset. Its job uses a fresh HA identity and does not restore the removed fixed-table topology. See [lakehouse reset](lakehouse.md) for the explicit destructive command. No migration or compatibility adapter is supplied for that old model.
