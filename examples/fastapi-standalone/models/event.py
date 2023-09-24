import redis
from redis import asyncio as aioredis
from fastapi import Request
import time
import asyncio

from utils.factory import RL_MANAGER
from utils.common import Singleton


class EventModel(metaclass=Singleton):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
    # ------------------------ #
    # Business Logic Functions #
    # ------------------------ #
                
    def sync_event_without_rl(self, *args, **kwargs):
        """Sync Example Function without lock release."""
        
        param1 = kwargs.get('param1')
        param2 = kwargs.get('param2')
        req: Request = kwargs.get('req')
        
        lock_key = "param1:{param1}&param2:{param2}.lock".format(param1=param1, param2=param2)
        lock_extender_suffix = "lock_extender"
        
        # Get aioredis_client from app state
        # aioredis_client: aioredis.Redis = req.app.state.aioredis
        
        # Get redis_client from app state
        # redis_client: redis.Redis = req.app.state.redis
        
        # We can initialize a lock release manager here if we wish
        # RL_MANAGER = HaredisLockRelaseManager(
        #    redis_client,
        #    aioredis_client,
        #    redis_logger=set_up_logging(console_log_level="DEBUG", logfile_log_level="DEBUG")
        #    )
        
        print("Main sync function is running...")
        
        time.sleep(10)
        
        result = param1 ** param2
        
        if result == 0:
            return {"multiply": {}}
            
        if param2 == 0:
            raise Exception("Division by zero!")
        
        result = {"multiply": result}
                
        return result
    
  
    async def async_event_without_rl(self, *args, **kwargs):
        """Async Example Function without lock release."""
        
        param1 = kwargs.get('param1')
        param2 = kwargs.get('param2')
        req: Request = kwargs.get('req')
        
        lock_key = "param1:{param1}&param2:{param2}.lock".format(param1=param1, param2=param2)
        lock_extender_suffix = "lock_extender"
        
        # Get aioredis_client from app state
        # aioredis_client: aioredis.Redis = req.app.state.aioredis
        
        # Get redis_client from app state
        # redis_client: redis.Redis = req.app.state.redis
        
        # We can initialize a lock release manager here if we wish
        # RL_MANAGER = HaredisLockRelaseManager(
        #    redis_client,
        #    aioredis_client,
        #    redis_logger=set_up_logging(console_log_level="DEBUG", logfile_log_level="DEBUG")
        #    )
        
        print("Main Async function is running...")
        
        await asyncio.sleep(10)
        
        result = param1 ** param2
        
        if result == 0:
            return {"multiply": {}}
            
        if param2 == 0:
            raise Exception("Division by zero!")
        
        result = {"multiply": result}
                
        return result
    
    # ------------------------------------------------ #
    # Business Logic + RL Decorated Combined Functions #
    # ------------------------------------------------ #
    
    @RL_MANAGER.aio_lock_release_decorator_streams(
        keys_to_lock=("param1", "param2"),
        lock_key_prefix=None,
        lock_expire_time=30,
        consumer_blocking_time=1000 * 2,
        consumer_do_retry=True,
        consumer_retry_count=5,
        consumer_retry_blocking_time_ms=2 * 1000,
        consumer_max_re_retry=2,
        null_handler={},
        run_with_lock_time_extender=True,
        lock_time_extender_suffix="lock_extender",
        lock_time_extender_add_time=10,
        lock_time_extender_blocking_time=5 * 1000,
        lock_time_extender_replace_ttl=True,
        delete_event_wait_time=10,
        redis_availability_strategy="error",
        response_cache=None,
        extend_cache_time=False
    )   
    async def async_event_decorated(self, *args, **kwargs):
        """Decorated Async Example Function with lock release."""
        
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
    
    @RL_MANAGER.aio_lock_release_decorator_streams(
        keys_to_lock=("param1", "param2"),
        lock_key_prefix=None,
        lock_expire_time=30,
        consumer_blocking_time=1000 * 2,
        consumer_do_retry=True,
        consumer_retry_count=5,
        consumer_retry_blocking_time_ms=2 * 1000,
        consumer_max_re_retry=2,
        null_handler={},
        run_with_lock_time_extender=True,
        lock_time_extender_suffix="lock_extender",
        lock_time_extender_add_time=10,
        lock_time_extender_blocking_time=5 * 1000,
        lock_time_extender_replace_ttl=True,
        delete_event_wait_time=10,
        redis_availability_strategy="error",
        response_cache=None,
        extend_cache_time=False
    )
    def sync_event_decorated(self, *args, **kwargs):
        """Decorated Async Example Function with lock release."""
        
        param1 = kwargs.get('param1')
        param2 = kwargs.get('param2')
        req: Request = kwargs.get('req')
        
        lock_key = "param1:{param1}&param2:{param2}.lock".format(param1=param1, param2=param2)
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
                
        return result
    
    # -------------------------------------------------------- #
    # Business Logic which wrapped from RL Decorator Functions #
    # -------------------------------------------------------- #
    
    @RL_MANAGER.aio_lock_release_decorator_streams(
        keys_to_lock=("param1", "param2"),
        lock_key_prefix=None,
        lock_expire_time=30,
        consumer_blocking_time=1000 * 2,
        consumer_do_retry=True,
        consumer_retry_count=5,
        consumer_retry_blocking_time_ms=2 * 1000,
        consumer_max_re_retry=2,
        null_handler={},
        run_with_lock_time_extender=True,
        lock_time_extender_suffix="lock_extender",
        lock_time_extender_add_time=10,
        lock_time_extender_blocking_time=5 * 1000,
        lock_time_extender_replace_ttl=True,
        delete_event_wait_time=10,
        redis_availability_strategy="error",
        response_cache=None,
        extend_cache_time=False
    )
    def sync_event_decorated_wrapped(self, *args, **kwargs):
        """Decorated Sync Example Wrapper Function with lock release."""
        return self.sync_event_without_rl(*args, **kwargs)
    
    @RL_MANAGER.aio_lock_release_decorator_streams(
        keys_to_lock=("param1", "param2"),
        lock_key_prefix=None,
        lock_expire_time=30,
        consumer_blocking_time=1000 * 2,
        consumer_do_retry=True,
        consumer_retry_count=5,
        consumer_retry_blocking_time_ms=2 * 1000,
        consumer_max_re_retry=2,
        null_handler={},
        run_with_lock_time_extender=True,
        lock_time_extender_suffix="lock_extender",
        lock_time_extender_add_time=10,
        lock_time_extender_blocking_time=5 * 1000,
        lock_time_extender_replace_ttl=True,
        delete_event_wait_time=10,
        redis_availability_strategy="error",
        response_cache=None,
        extend_cache_time=False
    )
    async def async_event_decorated_wrapped(self, *args, **kwargs):
        """Decorated Async Example Wrapper Function with lock release."""
        return await self.async_event_without_rl(*args, **kwargs)
    
    # ---------------------------------------------- #
    # Business Logic + RL Factory Combined Functions #
    # ---------------------------------------------- #

    async def async_event_rl_native(self, *args, **kwargs):
        
        # Get args and kwargs
        param1 = kwargs.get('param1')
        param2 = kwargs.get('param2')
        req: Request = kwargs.get('req')
        
        # Get aioredis_client from app state
        # aioredis_client: aioredis.Redis = req.app.state.aioredis
        
        # Get redis_client from app state
        # redis_client: redis.Redis = req.app.state.redis
        
        # We can initialize a lock release manager here if we wish
        # RL_MANAGER = HaredisLockRelaseManager(
        #    redis_client,
        #    aioredis_client,
        #    redis_logger=set_up_logging(console_log_level="DEBUG", logfile_log_level="DEBUG")
        #    )
                                        
        # call queue function from factory instead of initilized model
        result = await RL_MANAGER.aio_lock_release_with_stream(
            func=self.async_event_without_rl,
            keys_to_lock=("param1", "param2"),
            lock_key_prefix=None,
            lock_expire_time=30,
            consumer_blocking_time=1000 * 2,
            consumer_do_retry=True,
            consumer_retry_count=5,
            consumer_retry_blocking_time_ms=2 * 1000,
            consumer_max_re_retry=2,
            null_handler={},
            run_with_lock_time_extender=True,
            lock_time_extender_suffix="lock_extender",
            lock_time_extender_add_time=10,
            lock_time_extender_blocking_time=5 * 1000,
            lock_time_extender_replace_ttl=True,
            delete_event_wait_time=10,
            redis_availability_strategy="error",
            response_cache=None,
            extend_cache_time=False,
            args=("test"),
            **kwargs
        )
               
        return result
    
    async def sync_event_rl_native(self, *args, **kwargs):
        
        # Get args and kwargs
        param1 = kwargs.get('param1')
        param2 = kwargs.get('param2')
        req: Request = kwargs.get('req')
        
        # Get aioredis_client from app state
        # aioredis_client: aioredis.Redis = req.app.state.aioredis
        
        # Get redis_client from app state
        # redis_client: redis.Redis = req.app.state.redis
        
        # We can initialize a lock release manager here if we wish
        # RL_MANAGER = HaredisLockRelaseManager(
        #    redis_client,
        #    aioredis_client,
        #    redis_logger=set_up_logging(console_log_level="DEBUG", logfile_log_level="DEBUG")
        #    )
                                        
        # call queue function from factory instead of initilized model
        result = await RL_MANAGER.aio_lock_release_with_stream(
            func=self.sync_event_without_rl,
            keys_to_lock=("param1", "param2"),
            lock_key_prefix=None,
            lock_expire_time=30,
            consumer_blocking_time=1000 * 2,
            consumer_do_retry=True,
            consumer_retry_count=5,
            consumer_retry_blocking_time_ms=2 * 1000,
            consumer_max_re_retry=2,
            null_handler={},
            run_with_lock_time_extender=True,
            lock_time_extender_suffix="lock_extender",
            lock_time_extender_add_time=10,
            lock_time_extender_blocking_time=5 * 1000,
            lock_time_extender_replace_ttl=True,
            delete_event_wait_time=10,
            redis_availability_strategy="error",
            response_cache=100,
            extend_cache_time=True,
            args=("test"),
            **kwargs
        )
               
        return result