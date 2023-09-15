from fastapi import Request
import time
import asyncio

from haredis.client import RedisClient, AioRedisClient
from haredis.halock_manager import HAredlockManager
from haredis.config import set_up_logging

from utils.common import Singleton


class EventModel(metaclass=Singleton):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        
    def sync_event(self, *args, **kwargs):
        
        param1 = kwargs.get('param1')
        param2 = kwargs.get('param2')
        req: Request = kwargs.get('req')
        
        lock_key = f"param1:{param1}&param2:{param2}.lock"
        lock_extender_suffix = "lock_extender"
        
        redis_client: RedisClient = req.app.state.redis
        print("Main sync function is running...")
        
        time.sleep(10)
        
        result = param1 ** param2
        
        if result == 0:
            return {"multiply": {}}
            
        if param2 == 0:
            raise Exception("Division by zero!")
        
        result = {"multiply": result}
        
        # Close lock time extender with a produce event after retrieve result
        redis_client.client_conn.xadd(f"stream:{lock_key}.{lock_extender_suffix}", {"result": "end"}, maxlen=1)
        print("Lock extender closer event sended from main function.")
        
        return result
    
    async def async_event(self, *args, **kwargs):
        
        param1 = kwargs.get('param1')
        param2 = kwargs.get('param2')
        req: Request = kwargs.get('req')
        
        lock_key = f"param1:{param1}&param2:{param2}.lock"
        lock_extender_suffix = "lock_extender"
        
        aioredis_client: AioRedisClient = req.app.state.aioredis
        print("Main Async function is running...")
        
        await asyncio.sleep(10)
        
        result = param1 ** param2
        
        if result == 0:
            return {"multiply": {}}
            
        if param2 == 0:
            raise Exception("Division by zero!")
        
        result = {"multiply": result}
        
        # Close lock time extender with a produce event after retrieve result
        await aioredis_client.client_conn.xadd(f"stream:{lock_key}.{lock_extender_suffix}", {"result": "end"}, maxlen=1)
        print("Lock extender closer event sended from main function.")
        
        return result
    
    async def sync_xread_cache_queue(self, *args, **kwargs):
        
        # Get args and kwargs
        param1 = kwargs.get('param1')
        param2 = kwargs.get('param2')
        req: Request = kwargs.get('req')
        
        aioredis_client: AioRedisClient = req.app.state.aioredis
        
        # Assign lock and cache keys
        lock_key = f"param1:{param1}&param2:{param2}.lock"
                                
        # Initialize RedisLogger
        logger = set_up_logging()
                        
        # Init RedisLockReleaseManager
        rl_manager = HAredlockManager(aioredis_client=aioredis_client, redis_logger=logger)

        # call queue function
        result = await rl_manager.aio_lock_release_with_stream(
            function_name=self.sync_event,
            lock_key=lock_key,
            lock_extender_suffix="lock_extender",
            blocking_time_ms=1000 * 2,
            null_handler={},
            expire_time=30,
            wait_time=10,
            additional_time=10,
            replace_ttl = True,
            args=("test"),
            **kwargs
        )
               
        return result
    
    async def async_xread_cache_queue(self, *args, **kwargs):
        
        # Get args and kwargs
        param1 = kwargs.get('param1')
        param2 = kwargs.get('param2')
        req: Request = kwargs.get('req')
        
        aioredis_client: AioRedisClient = req.app.state.aioredis
        
        # Assign lock and cache keys
        lock_key = f"param1:{param1}&param2:{param2}.lock"
                                
        # Initialize RedisLogger
        logger = set_up_logging()
                        
        # Init RedisLockReleaseManager
        rl_manager = HAredlockManager(aioredis_client=aioredis_client, redis_logger=logger)
        # call queue function
        result = await rl_manager.aio_lock_release_with_stream(
            function_name=self.async_event,
            lock_key=lock_key,
            lock_extender_suffix="lock_extender",
            blocking_time_ms=1000 * 2,
            null_handler={},
            expire_time=30,
            wait_time=10,
            additional_time=10,
            replace_ttl = True,
            args=("test"),
            **kwargs
        )
               
        return result