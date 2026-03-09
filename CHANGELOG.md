# Changelog

## 2026-03-09

### Added
- Full CI quality gates (lint, type, tests with coverage) plus a separate 3-node integration smoke job.
- Coverage guardrail enforcement for core Raft/peer transport modules (now set to 85%).
- Operations runbook (`docs/runbook.md`) for readiness triage, leader failover, and snapshot/WAL recovery.
- New regression/integration suites for transport failures, quorum matrix behavior, restart/rejoin continuity, snapshot/compaction, and large-log recovery.

### Changed
- Node API routes cleaned up: placeholder handlers removed, runtime state endpoint added, legacy node set/get endpoints deprecated.
- Internal readiness semantics strengthened to reflect actual runtime safety (Raft invariants + peer/quorum conditions).
- Peer RPC diagnostics standardized with structured failure context and categorized metrics.
- Graceful shutdown now closes peer HTTP sessions via app/node lifecycle hooks.

### Fixed
- Majority commit behavior in write replication path to avoid false commits below quorum.
- Client-facing write error message typos in Raft node responses.
