# KVStore

A distributed key-value store with Raft consensus and HTTP-based node communication.

Recent project updates are tracked in `CHANGELOG.md`.

## Overview

KVStore provides:
- Raft leader election and term-based role transitions
- Majority-based write replication with append/heartbeat coordination
- Write-ahead logging (WAL) for command durability
- FastAPI endpoints for client traffic and internal node-to-node traffic
- Runtime metrics for API and peer-RPC behavior

## HTTP Architecture

The system now uses HTTP for both client requests and inter-node coordination.

### Client API (`/client/v1`)
- `GET /client/v1/kv/{key}`: read value
- `POST /client/v1/kv`: write value (leader only)
- `DELETE /client/v1/kv/{key}`: delete key (leader only)

### Internal Raft API (`/internal/v1`)
- `GET /internal/v1/health`
- `GET /internal/v1/ready`
- `GET /internal/v1/leader`
- `POST /internal/v1/ping`
- `POST /internal/v1/vote`
- `POST /internal/v1/append`
- `POST /internal/v1/heartbeat`
- `GET /internal/v1/metrics`

### Node API (`/node`)
- `GET /node/state`: node runtime snapshot
- `POST /node/set`: deprecated (`410`, use `POST /client/v1/kv`)
- `GET /node/get`: deprecated (`410`, use `GET /client/v1/kv/{key}`)

## Core Modules

- `src/core/raft/node.py`: main Raft node behavior (roles, replication, commit/apply)
- `src/core/raft/log.py`: WAL append/replay
- `src/core/peer_http_client.py`: resilient peer RPC transport (timeouts, retries, structured errors)
- `src/api/main.py`: FastAPI app assembly and middleware
- `src/api/routes/`: route handlers (`client`, `internal`, `node`)

## Project Layout

- `src/api/`: HTTP layer (`routes/`, request `schemas/`, shared route `services/`)
- `src/core/`: Raft + transport + metrics internals
- `tests/unit/`: unit-level coverage by subsystem (`api/`, `core/`, `rpc/`)
- `tests/integration/`: multi-node and durability/regression scenarios

## Running Locally

### 1) Install dependencies
```bash
uv sync
```

### 2) Start a single node
```bash
uv run python main.py
```

You can override runtime settings with env vars, for example:
```bash
NODE_ID=0 API_PORT=8080 LOCAL_DEV=true CLUSTER_SIZE=3 BASE_PORT=8080 uv run python main.py
```

### 3) Start a local 3-node cluster
```bash
uv run python local_cluster.py
```

## Running Tests

```bash
uv run pytest -q
```

(Current baseline: full suite passes.)

Large-log recovery baseline test:
```bash
uv run pytest -q -s tests/integration/test_large_log_recovery.py
```

Latest local measurement (March 9, 2026):  
`RECOVERY_BASELINE_NO_SNAPSHOT entries=3000 seconds=0.044`

## Configuration

Environment variables are defined in `src/core/config.py`.

### Node/cluster
- `NODE_ID`
- `NODE_PORT`
- `NODE_HOST`
- `API_PORT`
- `CLUSTER_SIZE`
- `POD_NAME`
- `SERVICE_NAME`
- `NAMESPACE`
- `DATA_DIR`
- `PEER_DISCOVERY_TIMEOUT`

### Logging/API
- `LOGS_DIR`
- `LOG_TO_FILE`
- `LOG_TO_CONSOLE`
- `LOG_LEVEL`
- `RPC_DEBUG`
- `ENABLE_DOCS`

### Peer RPC reliability
- `RPC_HTTP_CONNECT_TIMEOUT_SEC`
- `RPC_HTTP_READ_TIMEOUT_SEC`
- `RPC_HTTP_TOTAL_TIMEOUT_SEC`
- `RPC_HTTP_MAX_RETRIES`
- `RPC_HTTP_RETRY_BACKOFF_BASE_SEC`
- `RPC_HTTP_RETRY_BACKOFF_MAX_SEC`

### CORS
- `CORS_ALLOW_CREDENTIALS`
- `CORS_ORIGINS`
- `CORS_ALLOW_METHODS`
- `CORS_ALLOW_HEADERS`

## Incident Triage (Election/Replication)

When investigating cluster instability, use structured peer-RPC diagnostics:

- Log keys (failure/retry): `node_id`, `peer_id`, `rpc_type`, `attempt`, `term`, `category`
- Categories: `transport_error`, `http_error`, `decode_error`, `unexpected_error`
- Metrics counters by category:
  - `peer_rpc.<rpc_type>.count.ok`
  - `peer_rpc.<rpc_type>.count.transport_error`
  - `peer_rpc.<rpc_type>.count.http_error`
  - `peer_rpc.<rpc_type>.count.decode_error`
  - `peer_rpc.<rpc_type>.count.unexpected_error`

Recommended first checks:
1. `GET /internal/v1/metrics` and compare failure counters by RPC type.
2. Search logs for `peer_rpc_failure` and `peer_rpc_retry`.
3. Correlate `term` + `rpc_type=REQUEST_VOTE|APPEND_ENTRY|HEARTBEAT` for election/replication issues.

Extended operational playbooks (failover + snapshot recovery):
- `docs/runbook.md`

## Contributor Notes

Before opening a PR, run the same quality gates enforced in CI:

```bash
uv sync --all-groups
uv run ruff check .
uv run mypy
uv run pytest -q --cov --cov-report=term-missing
```

Coverage guardrail: CI enforces a minimum of 85% on `src/core/raft` plus `src/core/peer_http_client.py`.

CI also runs a separate 3-node integration smoke job (`scripts/ci_smoke.sh`) validating ready + leader write/read flow and uploading smoke logs on failure.

To block PR merges on failure, set branch protection on `main` and require both `Test, Lint, Type` and `Integration Smoke` status checks.
