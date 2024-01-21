from redis.asyncio import Redis as AsyncRedis
from typing import Any
from .xcom_backend import XcomBackend


class AsyncRedisBackend(XcomBackend):
    def __init__(self, host: str = None, port: int = None, db: int = None):
        super().__init__()
        self.redis = AsyncRedis(host=host, port=port, db=db)

    async def push(self, key: str, value: Any) -> None:
        await self.redis.set(key, self.to_bytes(value))

    async def pull(self, key: str) -> Any:
        value = await self.redis.get(key)
        if value is None:
            raise Exception(f"Failed to find key: {key}")
        return self.from_bytes(value)

    def teardown(self):
        self.redis.close()
