from typing import Any
import time

from fastapi import APIRouter, Request, HTTPException

from ...core.logging import get_logger
from ...core.metrics import get_metrics
from ...core.types import Command
from ..schemas import KvSetRequest
from ..services import leader_unavailable_error, replicate_and_commit

logger = get_logger(__name__)

router = APIRouter(tags=["client"])


def get_node(request: Request):
    """Get node from app state."""
    node = request.app.state.node
    if node is None:
        raise HTTPException(status_code=503, detail="Node not initialized")
    return node


@router.get("/kv/{key}")
async def get_value(request: Request, key: str) -> dict[str, Any]:
    """Read a value by key."""
    metrics = get_metrics()
    start_time = time.perf_counter()

    node = get_node(request)
    val = node.store.value_store.get(key)

    duration_ms = (time.perf_counter() - start_time) * 1000
    metrics.record_timing_sync("kv_store_read_latency_ms", duration_ms)

    if val is None:
        metrics.increment_counter_sync("kv_store_operations.get.not_found")
        raise HTTPException(status_code=404, detail=f"Key not found: {key}")

    metrics.increment_counter_sync("kv_store_operations.get.success")
    return {"key": key, "val": val}


@router.post("/kv")
async def set_value(request: Request, body: KvSetRequest) -> dict[str, Any]:
    """Set a key-value pair."""
    node = get_node(request)

    logger.debug(
        f"Write request: node.id={node.id}, role_state.role={node.role_state.role}, is_leader={node.is_leader}"
    )

    if not node.is_leader:
        logger.warning("Write rejected: node %s is not leader (role=%s)", node.id, node.role_state.role)
        raise leader_unavailable_error(node)

    metrics = get_metrics()
    start_time = time.perf_counter()

    cmd = Command(op="SET", key=body.key, val=body.val)
    if await replicate_and_commit(node, cmd, logger):

        duration_ms = (time.perf_counter() - start_time) * 1000
        metrics.record_timing_sync("raft_replication_latency_ms", duration_ms)
        metrics.increment_counter_sync("kv_store_operations.set.success")

        return {"status": "ok", "key": body.key, "val": body.val}

    metrics.increment_counter_sync("kv_store_operations.set.failure")
    raise HTTPException(status_code=500, detail="Failed to replicate to majority")


@router.delete("/kv/{key}")
async def delete_value(request: Request, key: str) -> dict[str, Any]:
    """Delete a key."""
    node = get_node(request)

    if not node.is_leader:
        raise leader_unavailable_error(node)

    cmd = Command(op="DELETE", key=key, val=None)
    if await replicate_and_commit(node, cmd, logger):
        return {"status": "ok", "key": key}

    raise HTTPException(status_code=500, detail="Failed to replicate to majority")
