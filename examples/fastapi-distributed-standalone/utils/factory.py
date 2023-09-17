import redis
from redis import asyncio as aioredis

from haredis import HaredisLockRelaseManager
from haredis import set_up_logging

from config import RedisSettings


_REDIS = redis.Redis(
    host="0.0.0.0",
    port=RedisSettings.PORT,
    db=RedisSettings.DB,
    password=RedisSettings.PASSWORD,
    decode_responses=True,
    encoding="utf-8",
    max_connections=2**31
)
_AIOREDIS = aioredis.Redis(
    host="0.0.0.0",
    port=RedisSettings.PORT,
    db=RedisSettings.DB,
    password=RedisSettings.PASSWORD,
    decode_responses=True,
    encoding="utf-8",
    max_connections=2**31
)

RL_MANAGER = HaredisLockRelaseManager(
    _REDIS,
    _AIOREDIS,
    redis_logger=set_up_logging(console_log_level="DEBUG", logfile_log_level="DEBUG")
    )