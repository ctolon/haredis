import redis
from redis import asyncio as aioredis

from haredis.client import AioHaredisClient, HaredisClient
from haredis.halock_manager import HaredisLockRelaseManager
from haredis.config import set_up_logging

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
    haredis_client=HaredisClient(client_conn=_REDIS),
    aioharedis_client=AioHaredisClient(client_conn=_AIOREDIS),
    redis_logger=set_up_logging(console_log_level="DEBUG", logfile_log_level="DEBUG")
    )