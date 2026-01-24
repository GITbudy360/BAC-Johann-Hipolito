from fastapi import FastAPI, HTTPException
from redis.asyncio.cluster import RedisCluster as AsyncRedisCluster

app = FastAPI()

# Connect to the cluster (using the docker service names)
# In K3s, these hostnames will be your Service names (e.g., redis-cluster-service)
startup_nodes = [
    {"host": "redis-node-1", "port": "6379"},
    {"host": "redis-node-2", "port": "6379"},
]

# Initialize global redis client
rc: AsyncRedisCluster = None

@app.on_event("startup")
async def startup_event():
    global rc
    # decode_responses=True ensures we get Strings back, not Bytes
    rc = AsyncRedisCluster(startup_nodes=startup_nodes, decode_responses=True)

@app.on_event("shutdown")
async def shutdown_event():
    if rc:
        await rc.aclose()

@app.get("/feed/{user_id}")
async def get_social_feed(user_id: str):
    """
    Simulates fetching a timeline.
    In a real app, this would get a list of post IDs, then fetch the posts.
    """
    # Simulate fetching 50 posts for this user
    # This is an I/O operation. FastAPI will handle other users while this runs.
    posts = await rc.lrange(f"timeline:{user_id}", 0, 50)
    
    if not posts:
        return {"message": "No posts found", "data": []}
    
    return {"user": user_id, "feed": posts}

@app.post("/post/")
async def create_post(user_id: str, content: str):
    """
    Simulates a user posting a status update.
    """
    # Push to the user's timeline list
    # LPUSH adds to the head of the list (newest first)
    await rc.lpush(f"timeline:{user_id}", content)

    # Keep list trimmed to 100 items to save memory
    await rc.ltrim(f"timeline:{user_id}", 0, 99)

    return {"status": "posted"}