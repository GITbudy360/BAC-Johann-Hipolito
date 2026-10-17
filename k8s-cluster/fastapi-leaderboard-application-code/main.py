from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from redis.asyncio.cluster import RedisCluster as AsyncRedisCluster
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialBackoff
from redis.exceptions import ConnectionError, TimeoutError, RedisClusterException
from prometheus_fastapi_instrumentator import Instrumentator
import os
import zlib
import asyncio

# Pull Redis configuration from environment variables (useful for Kubernetes)
REDIS_HOST = os.getenv("REDIS_HOST", "redis-master")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
# Define the number of shards used in the leaderboard. 120 is the lowest common multiple of 3 and 4 that provides enough granularity for state migration.
NUM_SHARDS = 120

# Initialize async Redis Cluster client
redis_client = AsyncRedisCluster(
    host=REDIS_HOST,
    port=REDIS_PORT,
    decode_responses=True,
    # Prevent crashes if some hash slots are temporarily migrating during re-sharding
    require_full_coverage=False,
    # Expands the socket pool to prevent exhaustion
    max_connections=4096,
    # Retry up to 3 times with an exponential backoff (e.g., 0.1s, 0.2s, 0.4s)
    retry=Retry(ExponentialBackoff(), retries=3),
    # Force a topology refresh and retry if these specific network errors occur
    retry_on_error=[ConnectionError, TimeoutError, RedisClusterException, OSError]
)

app = FastAPI(title="Global Leaderboard API")
# Exposes the /metrics endpoint
Instrumentator().instrument(app).expose(app)


class ScoreUpdate(BaseModel):
    score: float


def get_shard_key(player_id: str) -> str:
    # Deterministic hash to map a player consistently to one of the 12 shards
    shard_index = zlib.crc32(player_id.encode('utf-8')) % NUM_SHARDS
    return f"leaderboard:shard:{shard_index}"

@app.post("/player/{player_id}/score")
async def update_score(player_id: str, data: ScoreUpdate):
    shard_key = get_shard_key(player_id)
    await redis_client.zadd(shard_key, {player_id: data.score})
    return {"player_id": player_id, "score": data.score, "status": "updated", "shard": shard_key}

@app.get("/leaderboard")
async def get_leaderboard(top: int = 100):
    # Scatter: Query all 12 shards in parallel for their top players
    tasks = [
        redis_client.zrevrange(f"leaderboard:shard:{i}", 0, top - 1, withscores=True) 
        for i in range(NUM_SHARDS)
    ]
    shard_results = await asyncio.gather(*tasks)

    # Gather: Flatten the results, sort by score descending, and slice the true top N
    all_players = []
    for result in shard_results:
        for player_id, score in result:
            all_players.append({"player_id": player_id, "score": score})

    all_players.sort(key=lambda x: x["score"], reverse=True)
    global_top = all_players[:top]

    # Inject ranks
    for rank, player in enumerate(global_top, start=1):
        player["rank"] = rank

    return {"leaderboard": global_top}

@app.get("/player/{player_id}/rank")
async def get_player_rank(player_id: str):
    shard_key = get_shard_key(player_id)
    score = await redis_client.zscore(shard_key, player_id)

    if score is None:
        raise HTTPException(status_code=404, detail="Player not found in leaderboard")

    # Scatter: Ask every shard how many players have a strictly greater score
    tasks = [
        redis_client.zcount(f"leaderboard:shard:{i}", f"({score}", "+inf") 
        for i in range(NUM_SHARDS)
    ]
    higher_scores_counts = await asyncio.gather(*tasks)

    # Gather: The global rank is the sum of everyone above them, plus 1
    global_rank = sum(higher_scores_counts) + 1

    return {
        "player_id": player_id,
        "rank": global_rank,
        "score": score
    }
