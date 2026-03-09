#!/usr/bin/env bash
set -euo pipefail

BASE_PORT="${BASE_PORT:-18080}"
CLUSTER_SIZE=3
KEY="ci_smoke_key"
VALUE="ci_smoke_value"
LOG_DIR="${LOG_DIR:-.ci-smoke-logs}"
DATA_DIR_ROOT="${DATA_DIR_ROOT:-data/ci-smoke}"

mkdir -p "${LOG_DIR}" "${DATA_DIR_ROOT}"

PIDS=()
PYTHON_BIN=".venv/bin/python"
if [[ ! -x "${PYTHON_BIN}" ]]; then
  PYTHON_BIN="python"
fi

cleanup() {
  for pid in "${PIDS[@]:-}"; do
    if kill -0 "${pid}" >/dev/null 2>&1; then
      kill "${pid}" >/dev/null 2>&1 || true
      wait "${pid}" 2>/dev/null || true
    fi
  done
}
trap cleanup EXIT

echo "Starting ${CLUSTER_SIZE}-node smoke cluster on base port ${BASE_PORT}"
for i in $(seq 0 $((CLUSTER_SIZE - 1))); do
  port=$((BASE_PORT + i))
  node_data_dir="${DATA_DIR_ROOT}/node${i}"
  mkdir -p "${node_data_dir}"
  NODE_ID="${i}" \
  API_PORT="${port}" \
  NODE_HOST="127.0.0.1" \
  LOCAL_DEV="true" \
  CLUSTER_SIZE="${CLUSTER_SIZE}" \
  BASE_PORT="${BASE_PORT}" \
  DATA_DIR="${node_data_dir}" \
  "${PYTHON_BIN}" main.py >"${LOG_DIR}/node${i}.log" 2>&1 &
  PIDS+=("$!")
done

echo "Waiting for /health on all nodes..."
for i in $(seq 0 $((CLUSTER_SIZE - 1))); do
  port=$((BASE_PORT + i))
  for _ in $(seq 1 40); do
    if curl -fsS "http://127.0.0.1:${port}/internal/v1/health" >/dev/null; then
      break
    fi
    sleep 1
  done
  curl -fsS "http://127.0.0.1:${port}/internal/v1/health" >/dev/null
done

echo "Waiting for elected leader..."
LEADER_ID=""
for _ in $(seq 1 45); do
  for i in $(seq 0 $((CLUSTER_SIZE - 1))); do
    port=$((BASE_PORT + i))
    body="$(curl -fsS "http://127.0.0.1:${port}/internal/v1/leader" || true)"
    leader_id="$(python -c 'import json,sys; print(json.loads(sys.stdin.read()).get("leader_id"))' <<<"${body}" 2>/dev/null || true)"
    if [[ "${leader_id}" =~ ^[0-9]+$ ]]; then
      LEADER_ID="${leader_id}"
      break 2
    fi
  done
  sleep 1
done

if [[ -z "${LEADER_ID}" ]]; then
  echo "No leader elected in time"
  exit 1
fi
LEADER_PORT=$((BASE_PORT + LEADER_ID))
echo "Leader elected: id=${LEADER_ID}, port=${LEADER_PORT}"

echo "Checking /ready on all nodes..."
for i in $(seq 0 $((CLUSTER_SIZE - 1))); do
  port=$((BASE_PORT + i))
  for _ in $(seq 1 20); do
    if curl -fsS "http://127.0.0.1:${port}/internal/v1/ready" >/dev/null; then
      break
    fi
    sleep 1
  done
  curl -fsS "http://127.0.0.1:${port}/internal/v1/ready" >/dev/null
done

echo "Running write/read smoke flow through leader..."
curl -fsS -X POST \
  -H "Content-Type: application/json" \
  -d "{\"key\":\"${KEY}\",\"val\":\"${VALUE}\"}" \
  "http://127.0.0.1:${LEADER_PORT}/client/v1/kv" >/dev/null

for i in $(seq 0 $((CLUSTER_SIZE - 1))); do
  port=$((BASE_PORT + i))
  success="false"
  for _ in $(seq 1 20); do
    body="$(curl -fsS "http://127.0.0.1:${port}/client/v1/kv/${KEY}" || true)"
    read_val="$(python -c 'import json,sys; print(json.loads(sys.stdin.read()).get("val"))' <<<"${body}" 2>/dev/null || true)"
    if [[ "${read_val}" == "${VALUE}" ]]; then
      success="true"
      break
    fi
    sleep 1
  done
  if [[ "${success}" != "true" ]]; then
    echo "Node ${i} did not return replicated value"
    exit 1
  fi
done

echo "Smoke test passed."
