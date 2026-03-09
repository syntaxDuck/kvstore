from typing import Any

from fastapi import APIRouter, Request, HTTPException

from ...core.config import settings
from ...core.logging import get_logger
from ...core.metrics import get_metrics
from ...core.types import Command
from ...core.raft.role_state import Role

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
async def ready_check(request: Request) -> dict[str, Any]:
    node = request.app.state.node
    if node is None:
        raise HTTPException(status_code=503, detail="Not ready")

    issues: list[str] = []

    if node.role not in {Role.LEADER, Role.FOLLOWER, Role.CANDIDATE}:
        issues.append(f"invalid_role:{node.role}")
    if node.term < 0:
        issues.append("invalid_term")
    if node.commit_index > node.log.details.index:
        issues.append("commit_index_ahead_of_log")
    if node.last_applied > node.commit_index:
        issues.append("last_applied_ahead_of_commit")

    reachable_peers = 0
    peer_errors: list[str] = []
    for peer in node.peers:
        try:
            res = await peer.ping()
            if res.is_ok:
                reachable_peers += 1
            else:
                peer_errors.append(f"peer_{peer.id}:{res.payload}")
        except Exception as exc:
            peer_errors.append(f"peer_{peer.id}:{exc}")

    total_nodes = len(node.peers) + 1
    quorum = total_nodes // 2 + 1
    reachable_nodes = 1 + reachable_peers

    if node.is_leader and reachable_nodes < quorum:
        issues.append(
            f"leader_without_quorum:reachable_nodes={reachable_nodes},required={quorum}"
        )

    if (
        node.role in {Role.FOLLOWER, Role.CANDIDATE}
        and settings.CLUSTER_SIZE > 1
        and len(node.peers) > 0
        and reachable_peers == 0
    ):
        issues.append("no_reachable_peers")

    if issues:
        detail = {
            "status": "not_ready",
            "issues": issues,
            "reachable_peers": reachable_peers,
            "total_peers": len(node.peers),
            "peer_errors": peer_errors,
        }
        raise HTTPException(status_code=503, detail=detail)

    return {
        "status": "ready",
        "role": node.role,
        "term": node.term,
        "reachable_peers": reachable_peers,
        "total_peers": len(node.peers),
    }


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

    if prev_log_index > 0:
        prev_entry = node.log.replay_log_from(prev_log_index)
        if not prev_entry or prev_entry.term != prev_log_term:
            return {
                "success": False,
                "term": node.role_state.term,
                "last_log_index": node.log.details.index,
                "peer_id": node.id,
            }

    node.election_task.reset()

    if entries:
        for entry in entries:
            cmd = Command(**entry) if isinstance(entry, dict) else entry
            node.log.append(term, cmd)

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
