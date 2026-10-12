import asyncio
import os
import random
from redis.asyncio.cluster import RedisCluster as AsyncRedisCluster
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialBackoff
from redis.exceptions import ConnectionError, TimeoutError, RedisClusterException

# Default to the internal Kubernetes service DNS name
REDIS_HOST = os.getenv("REDIS_HOST", "redis-master-leader.redis-cluster.svc.cluster.local")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
LEADERBOARD_KEY = "global_leaderboard"
TOTAL_PLAYERS = 5000
BATCH_SIZE = 500


async def seed_data():
    print(f"Connecting to Redis cluster at {REDIS_HOST}:{REDIS_PORT}...")

    # Initialize client with the same retry logic used in your FastAPI application
    redis_client = AsyncRedisCluster(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True,
        require_full_coverage=False,
        retry=Retry(ExponentialBackoff(), retries=3),
        retry_on_error=[ConnectionError, TimeoutError, RedisClusterException, OSError]
    )

    print(f"Populating {TOTAL_PLAYERS} players into '{LEADERBOARD_KEY}'...")

    # Initialize a non-transactional pipeline to batch network requests
    pipe = redis_client.pipeline(transaction=False)

    for i in range(1, TOTAL_PLAYERS + 1):
        player_id = f"player_{i}"
        score = round(random.uniform(10.0, 10000.0), 2)

        # ZADD accepts a mapping of {member: score}
        pipe.zadd(LEADERBOARD_KEY, {player_id: score})

        # Execute the batch when we hit the chunk limit
        if i % BATCH_SIZE == 0:
            await pipe.execute()
            print(f"Inserted {i} players...")
            pipe = redis_client.pipeline(transaction=False)

    # Execute any remaining commands in the buffer
    if len(pipe) > 0:
        await pipe.execute()

    print("Data seeding complete.")
    await redis_client.aclose()

if __name__ == "__main__":
    asyncio.run(seed_data())
