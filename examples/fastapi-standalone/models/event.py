import redis
from redis import asyncio as aioredis
from fastapi import Request
import time
import asyncio

from haredis._client import AioHaredisClient, HaredisClient
from haredis.halock_manager import HaredisLockRelaseManager
from haredis.config import set_up_logging

from utils.factory import RL_MANAGER
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
        
        redis_client: redis.Redis = req.app.state.redis
        print("Main sync function is running...")
        
        time.sleep(10)
        
        result = param1 ** param2
        
        if result == 0:
            return {"multiply": {}}
            
        if param2 == 0:
            raise Exception("Division by zero!")
        
        result = {"multiply": result}
        
        # Close lock time extender with a produce event after retrieve result
        # redis_client.xadd(f"stream:{lock_key}.{lock_extender_suffix}", {"result": "end"}, maxlen=1)
        # print("Lock extender closer event sended from main function.")
        
        return result
    
  
    async def async_event(self, *args, **kwargs):
        
        param1 = kwargs.get('param1')
        param2 = kwargs.get('param2')
        req: Request = kwargs.get('req')
        
        lock_key = f"param1:{param1}&param2:{param2}.lock"
        lock_extender_suffix = "lock_extender"
        
        aioredis_client: aioredis.Redis = req.app.state.aioredis
        print("Main Async function is running...")
        
        await asyncio.sleep(10)
        
        result = param1 ** param2
        
        if result == 0:
            return {"multiply": {}}
            
        if param2 == 0:
            raise Exception("Division by zero!")
        
        result = {"multiply": result}
        
        # Close lock time extender with a produce event after retrieve result
        # await aioredis_client.xadd(f"stream:{lock_key}.{lock_extender_suffix}", {"result": "end"}, maxlen=1)
        # print("Lock extender closer event sended from main function.")
        
        return result
    
    @RL_MANAGER.aio_lock_release_decorator(
        keys_to_lock=("param1", "param2"),
        lock_key_prefix=None,
        lock_expire_time=30,
        consumer_blocking_time=1000 * 2,
        null_handler={},
        run_with_lock_time_extender=True,
        lock_time_extender_suffix="lock_extender",
        lock_time_extender_add_time=10,
        lock_time_extender_blocking_time=5 * 1000,
        lock_time_extender_replace_ttl=True,
        delete_event_wait_time=10,
    )   
    async def async_event_decorated(self, *args, **kwargs):
        
        param1 = kwargs.get('param1')
        param2 = kwargs.get('param2')
        req: Request = kwargs.get('req')
        
        print("Main Async function is running...")
        
        await asyncio.sleep(10)
        
        result = param1 ** param2
        
        if result == 0:
            return {"multiply": {}}
            
        if param2 == 0:
            raise Exception("Division by zero!")
        
        result = {"multiply": result}
                
        return result
    
    @RL_MANAGER.aio_lock_release_decorator(
        keys_to_lock=("param1", "param2"),
        lock_key_prefix=None,
        lock_expire_time=30,
        consumer_blocking_time=1000 * 2,
        null_handler={},
        run_with_lock_time_extender=True,
        lock_time_extender_suffix="lock_extender",
        lock_time_extender_add_time=10,
        lock_time_extender_blocking_time=5 * 1000,
        lock_time_extender_replace_ttl=True,
        delete_event_wait_time=10,
    )
    def sync_event_decorated(self, *args, **kwargs):
        
        param1 = kwargs.get('param1')
        param2 = kwargs.get('param2')
        req: Request = kwargs.get('req')
        
        lock_key = f"param1:{param1}&param2:{param2}.lock"
        lock_extender_suffix = "lock_extender"
        
        redis_client: redis.Redis = req.app.state.redis
        print("Main sync function is running...")
        
        time.sleep(10)
        
        result = param1 ** param2
        
        if result == 0:
            return {"multiply": {}}
            
        if param2 == 0:
            raise Exception("Division by zero!")
        
        result = {"multiply": result}
        
        # Close lock time extender with a produce event after retrieve result
        # redis_client.xadd(f"stream:{lock_key}.{lock_extender_suffix}", {"result": "end"}, maxlen=1)
        # print("Lock extender closer event sended from main function.")
        
        return result
    
    @RL_MANAGER.aio_lock_release_decorator(
        keys_to_lock=("param1", "param2"),
        lock_key_prefix=None,
        lock_expire_time=30,
        consumer_blocking_time=1000 * 2,
        null_handler={},
        run_with_lock_time_extender=True,
        lock_time_extender_suffix="lock_extender",
        lock_time_extender_add_time=10,
        lock_time_extender_blocking_time=5 * 1000,
        lock_time_extender_replace_ttl=True,
        delete_event_wait_time=10,
    )
    async def sync_event_decorated_2(self, *args, **kwargs):
        return await self.sync_event(*args, **kwargs)
    

    async def sync_xread_cache_queue(self, *args, **kwargs):
        
        # Get args and kwargs
        param1 = kwargs.get('param1')
        param2 = kwargs.get('param2')
        req: Request = kwargs.get('req')
        
        # Get aioharedis_client from app state
        aioharedis_client: AioHaredisClient = req.app.state.aioharedis
        
        # Get haredis_client from app state
        haredis_client: HaredisClient = req.app.state.haredis
        
        # Assign lock and cache keys
        lock_key = f"param1:{param1}&param2:{param2}.lock"
                                
        # Initialize RedisLogger
        logger = set_up_logging(console_log_level="DEBUG", logfile_log_level="DEBUG")
                        
        # Init RedisLockReleaseManager
        rl_manager = HaredisLockRelaseManager(
            aioharedis_client=aioharedis_client,
            haredis_client=haredis_client,
            redis_logger=logger
            )

        # call queue function
        result = await rl_manager.aio_lock_release_with_stream(
            function_name=self.sync_event,
            keys_to_lock=("param1", "param2"),
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
        
        # Get aioharedis_client from app state
        aioharedis_client: AioHaredisClient = req.app.state.aioharedis
        
        # Assign lock and cache keys
        lock_key = f"param1:{param1}&param2:{param2}.lock"
                                
        # Initialize RedisLogger
        logger = set_up_logging(console_log_level="DEBUG", logfile_log_level="DEBUG")
                        
        # Init RedisLockReleaseManager
        rl_manager = HaredisLockRelaseManager(
            aioharedis_client=aioharedis_client,
            redis_logger=logger
            )
        
        # call queue function
        result = await rl_manager.aio_lock_release_with_stream(
            function_name=self.async_event,
            keys_to_lock=("param1", "param2"),
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