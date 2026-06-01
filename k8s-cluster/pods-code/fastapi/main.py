import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from redis.asyncio.cluster import RedisCluster 

app = FastAPI(title="Leaderboard Entrypoint")

# Connect to the Headless Service you created earlier
REDIS_HOST = os.getenv("REDIS_HOST", "redis-headless.redis.svc.cluster.local") 
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

# Initialize the async Redis Cluster client.
# It automatically handles connection pooling and discovers other nodes via the headless service.
r = RedisCluster(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

class ScoreUpdate(BaseModel):
    player_id: str
    score: float

@app.post("/score/")
async def update_score(data: ScoreUpdate):
    try:
        # ZADD adds or updates a member's score in a sorted set (perfect for leaderboards)
        await r.zadd("global_leaderboard", {data.player_id: data.score})
        return {"status": "success", "player": data.player_id, "score": data.score}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/leaderboard/")
async def get_leaderboard(top: int = 10):
    try:
        # ZREVRANGE gets the top players by score descending
        leaders = await r.zrevrange("global_leaderboard", 0, top - 1, withscores=True)
        return {"leaderboard": [{"player_id": p, "score": s} for p, s in leaders]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/healthy")
async def health_check():
    return {"status": "healthy"}