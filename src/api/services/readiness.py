import asyncio
from typing import Any


async def collect_peer_readiness(peers, timeout_sec: float) -> tuple[int, list[str]]:
    reachable_peers = 0
    peer_errors: list[str] = []

    max_parallel_pings = max(1, min(len(peers), 10))
    ping_semaphore = asyncio.Semaphore(max_parallel_pings)

    async def ping_peer(peer) -> tuple[int, bool, Any]:
        async with ping_semaphore:
            try:
                res = await asyncio.wait_for(peer.ping(), timeout=timeout_sec)
                if res.is_ok:
                    return peer.id, True, None
                return peer.id, False, res.payload
            except Exception as exc:
                return peer.id, False, str(exc)

    ping_results = await asyncio.gather(*(ping_peer(peer) for peer in peers))
    for peer_id, is_ok, err in ping_results:
        if is_ok:
            reachable_peers += 1
        else:
            peer_errors.append(f"peer_{peer_id}:{err}")

    return reachable_peers, peer_errors
