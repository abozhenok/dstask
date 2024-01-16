from redis import Redis
from typing import Any, Union
from .xcom_backend import XcomBackend


class RedisBackend(XcomBackend):

    def __init__(self, host: str = None, port: int = None, db: Union[int, str] = None):
        super().__init__()
        self.redis = Redis(host=host, port=port, db=db)

    def push(self, key: str, value: Any) -> None:
        self.redis.set(key, self.to_bytes(value))

    def pull(self, key: str) -> Any:
        value = self.redis.get(key)
        if value is None:
            raise Exception(f"Failed to find key: {key}")
        return self.from_bytes(value)

    def teardown(self):
        self.redis.close()



