from typing import Any

from fastapi import APIRouter, Request, HTTPException

from ...core.logging import get_logger
from ...core.metrics import get_metrics

logger = get_logger(__name__)

router = APIRouter(tags=["internal"])


def get_node(request: Request):
    """Get node from app state."""
    node = request.app.state.node
    if node is None:
        raise HTTPException(status_code=503, detail="Node not initialized")
    return node


@router.get("/health")
async def health_check(request: Request) -> dict[str, str]:
    return {"status": "healthy"}


@router.get("/ready")
async def ready_check(request: Request) -> dict[str, str]:
    node = request.app.state.node
    if node is None:
        raise HTTPException(status_code=503, detail="Not ready")
    return {"status": "ready"}


@router.get("/leader")
async def leader_info(request: Request) -> dict[str, Any]:
    node = get_node(request)

    if node.is_leader:
        return {
            "leader_id": node.id,
            "address": f"{node.host}:{node.port}",
        }

    return {"leader_id": None, "status": "unknown"}


class PingRequest:
    pass


@router.post("/ping")
async def ping(request: Request) -> dict[str, Any]:
    """Peer registration endpoint."""
    node = get_node(request)
    logger.debug(
        f"Ping from node {node.id}: role_state.role={node.role_state.role}, is_leader={node.is_leader}"
    )
    return {
        "status": "ok",
        "node_id": node.id,
        "role": node.role,
    }


class VoteRequest:
    pass


@router.post("/vote")
async def request_vote(
    request: Request,
    candidate_id: int,
    term: int,
    last_log_index: int,
    last_log_term: int,
) -> dict[str, Any]:
    """Election vote request."""
    node = get_node(request)

    if term > node.role_state.term:
        node.role_state.become_follower(term)

    vote = node.get_vote_decision(candidate_id, term, last_log_index, last_log_term)
    node.role_state.voted_for = candidate_id if vote else None

    return {
        "vote": vote,
        "term": node.role_state.term,
        "node_id": node.id,
    }


class AppendRequest:
    pass


@router.post("/append")
async def append_entries(
    request: Request,
    leader_id: int,
    term: int,
    prev_log_index: int,
    prev_log_term: int,
    entries: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Log replication append entries."""
    node = get_node(request)

    if term < node.role_state.term:
        return {
            "success": False,
            "term": node.role_state.term,
        }

    if term > node.role_state.term:
        node.role_state.become_follower(term)

    node.election_task.reset()

    if entries:
        for entry in entries:
            node.log.append(term, entry)

    return {
        "success": True,
        "term": node.role_state.term,
        "last_log_index": node.log.details.index,
        "peer_id": node.id,
    }


@router.post("/heartbeat")
async def heartbeat(
    request: Request,
    leader_id: int,
    term: int,
    last_log_index: int,
    last_log_term: int,
    commit_index: int = 0,
) -> dict[str, Any]:
    """Leader heartbeat/keepalive."""
    node = get_node(request)

    if term > node.role_state.term:
        node.role_state.become_follower(term)

    node.election_task.reset()

    if commit_index > 0 and commit_index > node.commit_index:
        node.commit_index = commit_index
        node._apply_committed()

    return {
        "success": True,
        "term": node.role_state.term,
    }


@router.get("/metrics")
async def get_metrics_endpoint() -> dict[str, Any]:
    """Get collected metrics."""
    metrics = get_metrics()
    return metrics.get_all_metrics()
