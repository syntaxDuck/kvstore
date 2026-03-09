# KVStore Operations Runbook

This runbook covers high-signal checks and recovery steps for common incidents.

## 1) Cluster Readiness Triage

Use this when `/internal/v1/ready` returns `503` or traffic appears degraded.

1. Check each node:
   - `GET /internal/v1/health`
   - `GET /internal/v1/ready`
   - `GET /internal/v1/leader`
2. Check per-node counters:
   - `GET /internal/v1/metrics`
   - inspect `peer_rpc.<rpc_type>.count.<category>`
3. Inspect logs for:
   - `peer_rpc_retry`
   - `peer_rpc_failure`
   - `rpc_type=REQUEST_VOTE|APPEND_ENTRY|HEARTBEAT`
4. Confirm quorum viability:
   - a leader must reach quorum peers
   - follower/candidate should still reach at least one peer

If readiness failure is caused by an isolated minority outage, keep traffic on ready nodes.

## 2) Leader Failover Playbook

Use this when the leader crashes, is partitioned, or is unstable.

1. Identify current term/leader from `/internal/v1/leader` and logs.
2. Wait for election timeout window, then verify a single stable leader emerges.
3. Ensure the new leader can replicate writes:
   - perform a write via `POST /client/v1/kv`
   - read back via `GET /client/v1/kv/{key}`
4. If election flaps continue:
   - check `REQUEST_VOTE` and `HEARTBEAT` transport categories in metrics/logs
   - verify network connectivity and timeout settings
   - remove/cordon unhealthy node if it destabilizes quorum

## 3) Snapshot/WAL Recovery Playbook

Use this when a node restarts slowly, has large logs, or state divergence is suspected.

1. Stop the affected node.
2. Preserve data directory before intervention.
3. Start the node and observe startup logs for:
   - snapshot load
   - WAL replay
4. Validate state:
   - compare committed key set with leader after catch-up
   - verify `commit_index`/`last_applied` advance normally
5. If replay/snapshot errors occur:
   - restore from last known-good persistent volume backup
   - rejoin node and allow leader replication catch-up

## 4) Post-Incident Checklist

1. Capture failing metrics counters and log excerpts.
2. Record exact config values used during incident (`RPC_HTTP_*`, snapshot settings).
3. Add/extend regression tests for the observed failure mode.
4. Update this runbook with any newly discovered diagnostics.
