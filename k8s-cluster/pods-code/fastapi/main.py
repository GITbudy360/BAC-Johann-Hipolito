import asyncio
import logging
import os
from contextlib import asynccontextmanager
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from redis.asyncio.cluster import ClusterNode, RedisCluster
from redis.exceptions import RedisError

logger = logging.getLogger("uvicorn.error")

# Global placeholder, initialized inside the active event loop (see lifespan).
r: Optional[RedisCluster] = None


def _parse_startup_nodes() -> List[ClusterNode]:
    """Build the list of candidate startup nodes from REDIS_STARTUP_NODES.

    The default covers BOTH scaling scenarios, which live in the same `redis`
    namespace but expose different service names:
      - HPA StatefulSet      -> redis-headless         (redis-hpa-cluster.yaml)
      - Opstree RedisCluster -> redis-cluster-leader   (redis-operator-cluster.yaml)
    Only one scenario is deployed at a time; the other name simply fails DNS
    resolution and is skipped, so the SAME image works for both runs.
    """
    raw = os.getenv(
        "REDIS_STARTUP_NODES",
        "redis-headless.redis.svc.cluster.local:6379,"
        "redis-cluster-leader.redis.svc.cluster.local:6379",
    )
    nodes: List[ClusterNode] = []
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        host, _, port = item.partition(":")
        nodes.append(ClusterNode(host, int(port or 6379)))
    return nodes


async def _connect() -> RedisCluster:
    """Try each candidate startup node and return a client for the first that works."""
    last_err: Optional[Exception] = None
    for node in _parse_startup_nodes():
        try:
            client = RedisCluster(
                host=node.host,
                port=node.port,
                decode_responses=True,
                # Surface a saturated/broken cluster as a fast error instead of
                # hanging forever (which previously showed up as k6 timeouts).
                socket_timeout=3.0,
                socket_connect_timeout=3.0,
                # Tolerate brief topology gaps during scale-out / resharding.
                cluster_error_retry_attempts=3,
            )
            await client.ping()  # forces topology discovery
            logger.info("Connected to Redis Cluster via %s:%s", node.host, node.port)
            return client
        except Exception as e:  # noqa: BLE001 - DNS NXDOMAIN for the inactive scenario lands here
            last_err = e
            logger.warning("Startup node %s:%s unavailable: %s", node.host, node.port, e)
    raise RuntimeError(f"Could not connect to any Redis startup node: {last_err}")


async def _connect_with_retry() -> None:
    """Background task that keeps retrying until Redis is reachable.

    Runs OUTSIDE the blocking part of startup so uvicorn binds port 8000
    immediately and /healthy responds even while Redis is still coming up.
    """
    global r
    delay = 2.0
    while True:
        try:
            r = await _connect()
            return
        except Exception as e:  # noqa: BLE001
            logger.warning("Redis connect failed, retrying in %.1fs: %s", delay, e)
            await asyncio.sleep(delay)
            delay = min(delay * 1.5, 15.0)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global r
    # Connect to Redis in the BACKGROUND. Blocking here previously stopped uvicorn
    # from binding port 8000 while Redis was unreachable, so the liveness probe got
    # "connection refused" and kept killing the pod (Exit 137).
    connect_task = asyncio.create_task(_connect_with_retry())
    yield
    # Clean up on shutdown
    connect_task.cancel()
    if r is not None:
        await r.aclose()


app = FastAPI(title="Leaderboard Entrypoint", lifespan=lifespan)


class ScoreUpdate(BaseModel):
    board: str  # e.g. "lb:asia:ranked:2026-06-04" - spreads load across the keyspace
    player_id: str
    score: float


@app.post("/score/")
async def update_score(data: ScoreUpdate):
    if r is None:
        raise HTTPException(status_code=503, detail="redis not connected yet")
    try:
        # ZADD adds or updates a member's score in a sorted set (perfect for leaderboards).
        # `board` is high-cardinality so writes distribute across many slots/nodes.
        await r.zadd(data.board, {data.player_id: data.score})
        return {"status": "success", "board": data.board, "player": data.player_id, "score": data.score}
    except RedisError as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/leaderboard/")
async def get_leaderboard(board: str = Query(...), top: int = 10):
    if r is None:
        raise HTTPException(status_code=503, detail="redis not connected yet")
    try:
        # ZREVRANGE gets the top players by score descending.
        leaders = await r.zrevrange(board, 0, top - 1, withscores=True)
        return {"board": board, "leaderboard": [{"player_id": p, "score": s} for p, s in leaders]}
    except RedisError as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/healthy")
async def health_check():
    # Liveness: always ok as long as the process is up. Deliberately does NOT ping
    # Redis, so pods stay in rotation during cluster disruption and surface failures
    # as HTTP 500s (the metric the thesis wants to capture) instead of being evicted.
    return {"status": "healthy"}


@app.get("/ready")
async def readiness_check():
    # Readiness: only checks that the client was constructed, not live connectivity.
    if r is None:
        raise HTTPException(status_code=503, detail="redis client not initialized")
    return {"status": "ready"}
