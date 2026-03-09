from typing import Any
import time

from fastapi import APIRouter, Request, HTTPException
from pydantic import BaseModel

from ...core.logging import get_logger
from ...core.metrics import get_metrics
from ...core.types import Command, RpcRequest

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


def _leader_unavailable_error(node) -> HTTPException:
    leader_addr = node.leader_address
    return HTTPException(
        status_code=409,
        detail={
            "error": "not_leader",
            "leader_address": (
                {"host": leader_addr[0], "port": leader_addr[1]}
                if leader_addr
                else None
            ),
        },
    )


async def _replicate_and_commit(node, cmd: Command) -> bool:
    if len(node.peers) == 0:
        node.log.append(node.role_state.term, cmd)
        node.match_index[node.id] = node.log.details.index
        node._update_commit_index()
        return True

    next_index = node.log.details.index + 1
    responses = await node.send_to_all_peers(
        RpcRequest.append_entry(
            node.id,
            node.role,
            node.role_state.term,
            node.log.details.index,
            node.log.details.term,
            cmd,
        )
    )

    acked_peer_ids = [res.node_id for res in responses if res.is_ack]
    follower_acks = len(acked_peer_ids)
    logger.info(
        "Acknowledged by %s peers (need quorum %s)",
        follower_acks,
        node._quorum_size(),
    )
    if not node._has_majority(follower_acks):
        return False

    node.log.append(node.role_state.term, cmd)
    node.match_index[node.id] = node.log.details.index
    for peer_id in acked_peer_ids:
        node.update_match_index(peer_id, next_index)
    node._update_commit_index()
    return True


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
        raise _leader_unavailable_error(node)

    metrics = get_metrics()
    start_time = time.perf_counter()

    cmd = Command(op="SET", key=body.key, val=body.val)
    if await _replicate_and_commit(node, cmd):

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
        raise _leader_unavailable_error(node)

    cmd = Command(op="DELETE", key=key, val=None)
    if await _replicate_and_commit(node, cmd):
        return {"status": "ok", "key": key}

    raise HTTPException(status_code=500, detail="Failed to replicate to majority")
