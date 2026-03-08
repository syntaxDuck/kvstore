from typing import Any

from fastapi import APIRouter, Request, HTTPException
from pydantic import BaseModel

from ...core.logging import get_logger

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
    node = get_node(request)
    val = node.store.value_store.get(key)
    if val is None:
        raise HTTPException(status_code=404, detail=f"Key not found: {key}")
    return {"key": key, "val": val}


@router.post("/kv")
async def set_value(request: Request, body: KvSetRequest) -> dict[str, Any]:
    """Set a key-value pair."""
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

    cmd = Command(op="SET", key=body.key, val=body.val)

    responses = await node.send_to_all_peers_from_http(
        cmd,
        node.role_state.term,
        node.log.details.index,
        node.log.details.term,
    )

    count = sum(1 for r in responses if r.get("status") == "ACK")

    if count > len(node.peers) // 2:
        node.log.append(node.role_state.term, cmd)
        node.store.apply(cmd)
        return {"status": "ok", "key": body.key, "val": body.val}

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

    count = sum(1 for r in responses if r.get("status") == "ACK")

    if count > len(node.peers) // 2:
        node.log.append(node.role_state.term, cmd)
        node.store.apply(cmd)
        return {"status": "ok", "key": key}

    raise HTTPException(status_code=500, detail="Failed to replicate to majority")
