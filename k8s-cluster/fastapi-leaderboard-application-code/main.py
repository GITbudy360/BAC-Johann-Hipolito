from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import redis.asyncio as redis
import os
from redis.asyncio.cluster import RedisCluster as AsyncRedisCluster

# Pull Redis configuration from environment variables (useful for Kubernetes)
REDIS_HOST = os.getenv("REDIS_HOST", "redis-master")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

# Initialize async Redis Cluster client
redis_client = AsyncRedisCluster(
    host=REDIS_HOST, 
    port=REDIS_PORT, 
    decode_responses=True
)

app = FastAPI(title="Global Leaderboard API")

LEADERBOARD_KEY = "global_leaderboard"

class ScoreUpdate(BaseModel):
    score: float

@app.post("/player/{player_id}/score")
async def update_score(player_id: str, data: ScoreUpdate):
    # ZADD adds or updates the player in the Sorted Set with their new score
    await redis_client.zadd(LEADERBOARD_KEY, {player_id: data.score})
    return {"player_id": player_id, "score": data.score, "status": "updated"}

@app.get("/leaderboard")
async def get_leaderboard(top: int = 100):
    # ZREVRANGE fetches the highest scores in descending order
    results = await redis_client.zrevrange(LEADERBOARD_KEY, 0, top - 1, withscores=True)
    
    leaderboard = []
    for rank, (player_id, score) in enumerate(results, start=1):
        leaderboard.append({
            "rank": rank,
            "player_id": player_id,
            "score": score
        })
    return {"leaderboard": leaderboard}

@app.get("/player/{player_id}/rank")
async def get_player_rank(player_id: str):
    # ZREVRANK returns the 0-based index of the player, from highest to lowest score
    rank = await redis_client.zrevrank(LEADERBOARD_KEY, player_id)
    
    if rank is None:
        raise HTTPException(status_code=404, detail="Player not found in leaderboard")
        
    score = await redis_client.zscore(LEADERBOARD_KEY, player_id)
    
    return {
        "player_id": player_id,
        "rank": rank + 1,  # Convert 0-based index to 1-based rank (e.g., 1st place)
        "score": score
    }