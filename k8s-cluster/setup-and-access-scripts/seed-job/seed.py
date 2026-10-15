import asyncio
import os
import random
import zlib
from redis.asyncio.cluster import RedisCluster as AsyncRedisCluster
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialBackoff
from redis.exceptions import ConnectionError, TimeoutError, RedisClusterException

REDIS_HOST = os.getenv("REDIS_HOST", "redis-master-leader.redis-cluster.svc.cluster.local")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
TOTAL_PLAYERS = 5000
BATCH_SIZE = 500
# Define the number of shards used in the leaderboard. 120 is the lowest common multiple of 3 and 4 that provides enough granularity for state migration.
NUM_SHARDS = 120


def get_shard_key(player_id: str) -> str:
    shard_index = zlib.crc32(player_id.encode('utf-8')) % NUM_SHARDS
    return f"leaderboard:shard:{shard_index}"


async def seed_data():
    print(f"Connecting to Redis cluster at {REDIS_HOST}:{REDIS_PORT}...")

    redis_client = AsyncRedisCluster(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True,
        require_full_coverage=False,
        retry=Retry(ExponentialBackoff(), retries=3),
        retry_on_error=[ConnectionError, TimeoutError, RedisClusterException, OSError]
    )

    await redis_client.ping()
    print("Cluster topology initialized successfully.")
    print(f"Populating {TOTAL_PLAYERS} players across {NUM_SHARDS} shards...")

    # Initialize a non-transactional pipeline to batch network requests
    pipe = redis_client.pipeline(transaction=False)

    for i in range(1, TOTAL_PLAYERS + 1):
        player_id = f"player_{i}"
        score = round(random.uniform(10.0, 10000.0), 2)
        shard_key = get_shard_key(player_id)

        pipe.zadd(shard_key, {player_id: score})

        if i % BATCH_SIZE == 0:
            await pipe.execute()
            print(f"Inserted {i} players...")
            pipe = redis_client.pipeline(transaction=False)

    if len(pipe) > 0:
        await pipe.execute()

    print("Data seeding complete.")
    await redis_client.aclose()

if __name__ == "__main__":
    asyncio.run(seed_data())
