from fastapi import HTTPException

from ...core.types import Command, RpcRequest


def leader_unavailable_error(node) -> HTTPException:
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


async def replicate_and_commit(node, cmd: Command, logger) -> bool:
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
