from typing import Any

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel


router = APIRouter(prefix="/node", tags=["node"])


class NodeSetRequest(BaseModel):
    key: str
    val: Any


def get_node(request: Request):
    node = getattr(request.app.state, "node", None)
    if node is None:
        raise HTTPException(status_code=503, detail="Node not initialized")
    return node


@router.post("/set")
async def set_value(_: Request, __: NodeSetRequest) -> dict[str, str]:
    raise HTTPException(
        status_code=410,
        detail="Deprecated endpoint. Use POST /client/v1/kv instead.",
    )


@router.get("/get")
async def get_value(_: Request, key: str) -> dict[str, str]:
    raise HTTPException(
        status_code=410,
        detail=f"Deprecated endpoint for key '{key}'. Use GET /client/v1/kv/{{key}}.",
    )


@router.get("/state")
async def get_state(request: Request) -> dict[str, Any]:
    node = get_node(request)
    return {
        "node_id": node.id,
        "role": node.role,
        "term": node.term,
        "is_leader": node.is_leader,
        "commit_index": node.commit_index,
        "last_applied": node.last_applied,
        "peer_count": len(node.peers),
        "log_index": node.log.details.index,
        "log_term": node.log.details.term,
    }
