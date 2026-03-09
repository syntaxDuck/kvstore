from typing import Any
import time

from fastapi import APIRouter, Request, HTTPException
from pydantic import BaseModel

from ...core.logging import get_logger
from ...core.metrics import get_metrics

logger = get_logger(__name__)

router = APIRouter(tags=["client"])


class KvSetRequest(BaseModel):
    key: str
    val: Any


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
        logger.warning(
            f"Write rejected: node {node.id} is not leader (role={node.role_state.role})"
        )
        leader_addr = node.leader_address
        if leader_addr:
            raise HTTPException(
                status_code=307,
                detail=f"Not leader, redirect to {leader_addr[0]}:{leader_addr[1]}",
            )
        raise HTTPException(status_code=503, detail="No leader available")

    from ...core.types import Command

    metrics = get_metrics()
    start_time = time.perf_counter()

    cmd = Command(op="SET", key=body.key, val=body.val)

    logger.info(f"Write request: peers={len(node.peers)}, cmd={cmd}")

    responses = await node.send_to_all_peers_from_http(
        cmd,
        node.role_state.term,
        node.log.details.index,
        node.log.details.term,
    )

    logger.info(f"Replication responses: {responses}")
    follower_acks = sum(1 for r in responses if r.get("success") is True)

    logger.info(
        f"Acknowledged by {follower_acks} peers (need quorum {node._quorum_size()})"
    )

    if node._has_majority(follower_acks):
        node.log.append(node.role_state.term, cmd)
        node.match_index[node.id] = node.log.details.index

        for r in responses:
            if r.get("success") is True:
                peer_id = r.get("peer_id")
                match_idx = r.get("last_log_index", node.log.details.index)
                if peer_id is not None:
                    node.update_match_index(peer_id, match_idx)

        node._update_commit_index()

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
        leader_addr = node.leader_address
        if leader_addr:
            raise HTTPException(
                status_code=307,
                detail=f"Not leader, redirect to {leader_addr[0]}:{leader_addr[1]}",
            )
        raise HTTPException(status_code=503, detail="No leader available")

    from ...core.types import Command

    cmd = Command(op="DELETE", key=key, val=None)

    responses = await node.send_to_all_peers_from_http(
        cmd,
        node.role_state.term,
        node.log.details.index,
        node.log.details.term,
    )

    follower_acks = sum(1 for r in responses if r.get("success") is True)

    if node._has_majority(follower_acks):
        node.log.append(node.role_state.term, cmd)
        node.match_index[node.id] = node.log.details.index
        for r in responses:
            if r.get("success") is True:
                peer_id = r.get("peer_id")
                match_idx = r.get("last_log_index", node.log.details.index)
                if peer_id is not None:
                    node.update_match_index(peer_id, match_idx)
        node._update_commit_index()
        return {"status": "ok", "key": key}

    raise HTTPException(status_code=500, detail="Failed to replicate to majority")
