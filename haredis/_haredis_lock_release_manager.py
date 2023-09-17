from typing import Callable, Union, Any
import logging
import json
import asyncio
import functools
from functools import partial
import inspect
import sys

# For Asyncio Python 3.6 compatibility
if sys.version_info[:2] >= (3, 7):
    from asyncio import get_running_loop
else:
    from asyncio import _get_running_loop as get_running_loop

import redis
from redis import asyncio as aioredis

from ._base._base_lock_release_manager import _BaseLockRelaseManager

class HaredisLockRelaseManager(object):
    """## Redis Lock Release Manager Class for Distributed Caching/Locking in Redis
    This class is used to implement distributed locking in redis using stream api xread/xadd (For both asyncronous/syncronous execution).
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        aioredis_client: aioredis.Redis = None,
        redis_logger: Union[logging.Logger, None] = None
        ):
        """Constructor for RedisLockReleaseManager for Redis as Standalone.

        Args:
            aioharedis_client (AioHaredisClient): AioHaredisClient Instance.
            hairedis_client (HaredisClient, optional): HaredisClient Instance. Needed for syncronous execution. Defaults to None.
            redis_logger (logging.Logger, optional): Logger Instance. Defaults to None.
        """
        
        self.__rl_manager = _BaseLockRelaseManager(
            redis_client=redis_client,
            aioredis_client=aioredis_client,
            redis_logger=redis_logger
            )
        
    @property
    def rl_manager(self):
        return self.__rl_manager
        
    async def aio_lock_release_with_stream(   
        self,
        func: Callable,
        keys_to_lock: tuple,
        lock_key_prefix = None,
        lock_expire_time=30,
        consumer_blocking_time=5 * 1000,
        consumer_do_retry=True,
        consumer_retry_count=5,
        consumer_retry_blocking_time_ms=2 * 1000,
        consumer_max_re_retry=2,
        null_handler="null",
        run_with_lock_time_extender=True,
        lock_time_extender_suffix="lock_extender",
        lock_time_extender_add_time=10,
        lock_time_extender_blocking_time=5 * 1000,
        lock_time_extender_replace_ttl=True,
        delete_event_wait_time=10,
        args=tuple(),
        **kwargs
        ):
        """haredis distributed locking algorithm implementation in redis using stream api xread/xadd (For both syncronous/asyncronous execution).

        Args:
            func (Callable): Function name to be executed.
            keys_to_lock (tuple): Keys to be locked.
            lock_key_prefix (str, optional): Prefix for lock key. Defaults to None.
            lock_expire_time (int): Expiry time of the lock. Defaults to 30.
            consumer_blocking_time (int): Blocking time in milliseconds for consumers. Defaults to 5000.
            consumer_do_retry (bool): If True, consumer will be retried, if lock released. Defaults to True.
            consumer_retry_count (int): Retry count for consumer. Defaults to 5.
            consumer_retry_blocking_time_ms (int): Blocking time in milliseconds for consumer retry. Defaults to 2000.
            consumer_max_re_retry (int): Max re-retry count for consumer. Defaults to 2.
            null_handler (Any): Null handler for empty result (it can be {}, [] or null). Defaults to "null".
            run_with_lock_time_extender (bool): If True, lock time extender will be executed for High Availability. Defaults to True.
            lock_time_extender_suffix (str, optional): Suffix for lock extender stream key. Defaults to "lock_extender".
            lock_time_extender_add_time (int): Additional time for lock time extender to be executed. Defaults to 10.
            lock_time_extender_blocking_time (int): Blocking time in milliseconds for lock time extender. Defaults to 5000.
            lock_time_extender_replace_ttl (bool): Replace ttl of the lock. Defaults to True.
            delete_event_wait_time (int): Wait time for delete event operation. Defaults to 10.

        Returns:
            Any: Result of the function
        """
        
        result = None
        nullable = [{}, [], "null"]
        exception_string = None
        exception_found = False
        func_name = func.__name__
        
        if not isinstance(func, Callable):
            raise TypeError("func must be callable.")
        
        if not isinstance(lock_time_extender_suffix, str):
            raise TypeError("lock_time_extender_suffix must be string.")
                
        if null_handler not in nullable:
            raise Exception("null_handler must be type of one of these: {nullable}".format(nullable=nullable))
        
        if lock_expire_time < 0:
            raise ValueError("lock_expire_time must be greater than 0.")
        
        if not run_with_lock_time_extender:
            await self.rl_manager.warn_aio_lock_time_extender(
                lock_time_extender_suffix,
                lock_time_extender_add_time,
                lock_time_extender_blocking_time,
                lock_time_extender_replace_ttl
            )
                
        # Check if function is coroutine function, if not, check if haredis_client is provided
        is_main_coroutine = inspect.iscoroutinefunction(func)
        if not is_main_coroutine and not self.rl_manager.haredis_client:
            raise RuntimeError("haredis_client parameter must be provided in class constructor for syncronous execution.")
        
        lock_key = await self.rl_manager.lock_key_generator(keys_to_lock, args, kwargs, lock_key_prefix)
                    
        # Define stream key for consumer
        consumer_stream_key = "stream:{lock_key}".format(lock_key=lock_key)
        streams = {consumer_stream_key: "$"}
        
        try:
            loop = get_running_loop()
        except RuntimeError as err:
            self.rl_manager.redis_logger.debug("Event Loop Error: {err}".format(err=err))
            loop = asyncio.get_event_loop()
                
        # Acquire lock
        self.rl_manager.redis_logger.debug("Lock key: {lock_key} will be acquired.".format(lock_key=lock_key))
        lock = await self.rl_manager.aioharedis_client.acquire_lock(lock_key, lock_expire_time)
        is_locked = await self.rl_manager.aioharedis_client.is_locked(lock)
        is_owned = await self.rl_manager.aioharedis_client.is_owned(lock)
        
        self.rl_manager.redis_logger.debug("Lock key: {lock_key} is locked: {is_locked}, is owned: {is_owned}"
                                           .format(lock_key=lock_key, is_locked=is_locked, is_owned=is_owned))
                
        # If lock is not owned by current process, call consumer otherwise call producer         
        if is_owned:
            try:
                self.rl_manager.redis_logger.info("Lock key: {lock_key} acquired.".format(lock_key=lock_key))
                
                # lock_token = await self.rl_manager.aioharedis_client.client_conn.get(lock_key)
                # print("LOCK TOKEN :", lock_token)
                
                # TODO optimize them for run with lock time extender
                # Get Functions as partial for asyncio.gather or run_sync_async_parallel
                partial_main = await self.rl_manager.partial_main_selector(
                    func,
                    lock_key,
                    lock_time_extender_suffix,
                    is_main_coroutine,
                    run_with_lock_time_extender,
                    args, kwargs
                    )

                partial_lock_time_extender = partial(
                    self.rl_manager.aio_lock_time_extender,
                    lock,
                    consumer_stream_key,   
                    lock_time_extender_suffix,
                    lock_time_extender_add_time,
                    lock_time_extender_blocking_time,
                    lock_time_extender_replace_ttl
                    )
                
                
                # Define tasks for asyncio.gather
                tasks = [partial_main, partial_lock_time_extender]
                
                # Run function with lock time extender or without lock time extender
                if run_with_lock_time_extender:
                    self.rl_manager.redis_logger.debug("Function {func_name} will be executed with Lock time extender."
                                                       .format(func_name=func_name))
                    
                    # Check if function is coroutine function, if not, run it with run_sync_async_parallel, if yes, run it with asyncio.gather
                    if is_main_coroutine:
                        self.rl_manager.redis_logger.debug("Function {func_name} will be executed with asyncio.gather."
                                                           .format(func_name=func_name))
                        runner = await asyncio.gather(tasks[0](), tasks[1](), loop=loop, return_exceptions=False)
                    else:
                        self.rl_manager.redis_logger.debug("Function {func_name} will be executed with run_sync_async_parallel."
                                                           .format(func_name=func_name))
                        runner = await asyncio.gather(
                            loop.run_in_executor(None, partial_main),
                            self.rl_manager.aio_lock_time_extender(
                                lock,
                                consumer_stream_key,   
                                lock_time_extender_suffix,
                                lock_time_extender_add_time,
                                lock_time_extender_blocking_time,
                                lock_time_extender_replace_ttl
                            ),
                            return_exceptions=False
                        )
                        
                else:
                    self.rl_manager.redis_logger.debug("Function {func_name} will be executed without Lock time extender."
                                                       .format(func_name=func_name))
                    if is_main_coroutine:
                        runner = await asyncio.gather(partial_main(), loop=loop, return_exceptions=False)
                    else:
                        runner = await asyncio.gather(
                            loop.run_in_executor(None, partial_main),
                            return_exceptions=False
                        )
                    
                result = runner[0]
                self.rl_manager.redis_logger.debug("Result of the function: {result}".format(result=result))

            except Exception as e:
                exception_string = e.args[0]
                self.rl_manager.redis_logger.error("Exception: {exception_string}".format(exception_string=exception_string))
                result = exception_string
                exception_found = True
                
            finally:
                                
                if exception_string:
                    self.rl_manager.redis_logger.warning("Exception found {exception_string}".format(exception_string=exception_string))

                # Check if result is exception
                if exception_found:
                    self.rl_manager.redis_logger.error("Result is exception. Lock key: {lock_key} will be released. Exception: {result}"
                                                       .format(lock_key=lock_key, result=result))
                    raw_data = "RedException" + ":" + str(result)
                    event_data = {"result": raw_data}
                    await self.rl_manager.aioharedis_client.release_lock(lock)
                    self.rl_manager.redis_logger.info("Lock key: {lock_key} released.".format(lock_key=lock_key))
                    _ = await self.rl_manager.aioharedis_client.produce_event_xadd(stream_name=consumer_stream_key, data=event_data, maxlen=1)
                    event_info = await self.rl_manager.aioharedis_client.client_conn.xinfo_stream(consumer_stream_key)
                    event_id = event_info["last-entry"][0]
                    self.rl_manager.redis_logger.info("Event produced to notify consumers: {event_info}".format(event_info=event_info))

                    asyncio.ensure_future(self.rl_manager.aio_delete_event(consumer_stream_key, lock_key, event_id, event_info, delete_event_wait_time), loop=loop) 
                    return raw_data
                
                # Check if result is empty
                if result is None or result == {} or result == [] or result == "null":
                    self.rl_manager.redis_logger.warning("Result is empty. Lock key: {lock_key} will be released".format(lock_key=lock_key))
                    raw_data = "null"
                    event_data = {"result": raw_data}
                    await self.rl_manager.aioharedis_client.release_lock(lock)
                    self.rl_manager.redis_logger.info("Lock key: {lock_key} released.".format(lock_key=lock_key))
                    _ = await self.rl_manager.aioharedis_client.produce_event_xadd(stream_name=consumer_stream_key, data=event_data, maxlen=1)
                    event_info = await self.rl_manager.aioharedis_client.client_conn.xinfo_stream(consumer_stream_key)
                    event_id = event_info["last-entry"][0]
                    self.rl_manager.redis_logger.info("Event produced to notify consumers: {event_info}".format(event_info=event_info))
                    asyncio.ensure_future(self.rl_manager.aio_delete_event(consumer_stream_key, lock_key, event_id, event_info, delete_event_wait_time), loop=loop) 
                    return null_handler
                
                # If everything is ok, serialize data, release lock and finally produce event to consumers
                event_data = {"result": json.dumps(result)}
                await self.rl_manager.aioharedis_client.release_lock(lock)
                self.rl_manager.redis_logger.info("Lock key: {lock_key} released.".format(lock_key=lock_key))
                _ = await self.rl_manager.aioharedis_client.produce_event_xadd(stream_name=consumer_stream_key, data=event_data, maxlen=1)
                event_info = await self.rl_manager.aioharedis_client.client_conn.xinfo_stream(consumer_stream_key)
                event_id = event_info["last-entry"][0]
                # event_data = event_info["last-entry"][1]
                self.rl_manager.redis_logger.info("Event produced to notify consumers: {event_info}".format(event_info=event_info))
                asyncio.ensure_future(self.rl_manager.aio_delete_event(consumer_stream_key, lock_key, event_id, event_info, delete_event_wait_time), loop=loop) 
        else:
            
            # Call consumer if lock is not owned by current process
            result = await self.rl_manager.aiocall_consumer(
                lock_key,
                streams,
                consumer_blocking_time,
                null_handler,
                consumer_stream_key,
                consumer_do_retry,
                consumer_retry_count,
                consumer_retry_blocking_time_ms,
                consumer_max_re_retry,
                )
            return result
            
        return result
                        
    def aio_lock_release_decorator(
        self,
        keys_to_lock: tuple,
        lock_key_prefix = None,
        lock_expire_time=30,
        consumer_blocking_time=5000,
        consumer_do_retry=True,
        consumer_retry_count=5,
        consumer_retry_blocking_time_ms=2 * 1000,
        consumer_max_re_retry=2,
        null_handler="null",
        run_with_lock_time_extender=True,
        lock_time_extender_suffix="lock_extender",
        lock_time_extender_add_time=10,
        lock_time_extender_blocking_time=5000,
        lock_time_extender_replace_ttl=True,
        delete_event_wait_time=10,
    ) -> Any:
        """haredis distributed locking algorithm implementation in redis using stream api xread/xadd (For both syncronous/asyncronous execution) as decorator.

        Args:
            keys_to_lock (tuple): Keys to be locked.
            lock_key_prefix (str, optional): Prefix for lock key. Defaults to None.
            lock_expire_time (int): Expiry time of the lock. Defaults to 30.
            consumer_blocking_time (int): Blocking time in milliseconds for consumers. Defaults to 5000.
            consumer_do_retry (bool): If True, consumer will be retried, if lock released. Defaults to True.
            consumer_retry_count (int): Retry count for consumer. Defaults to 5.
            consumer_retry_blocking_time_ms (int): Blocking time in milliseconds for consumer retry. Defaults to 2000.
            consumer_max_re_retry (int): Max re-retry count for consumer. Defaults to 2.
            null_handler (Any): Null handler for empty result (it can be {}, [] or null). Defaults to "null".
            run_with_lock_time_extender (bool): If True, lock time extender will be executed for High Availability. Defaults to True.
            lock_time_extender_suffix (str, optional): Suffix for lock extender stream key. Defaults to "lock_extender".
            lock_time_extender_add_time (int): Additional time for lock time extender to be executed. Defaults to 10.
            lock_time_extender_blocking_time (int): Blocking time in milliseconds for lock time extender. Defaults to 5000.
            lock_time_extender_replace_ttl (bool): Replace ttl of the lock. Defaults to True.
            delete_event_wait_time (int): Wait time for delete event operation. Defaults to 10.

        Returns:
            Any: Result of the function
        """
        def decorator(func: Callable):
            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                result = await self.aio_lock_release_with_stream(
                    func,
                    keys_to_lock,
                    lock_key_prefix,
                    lock_expire_time,
                    consumer_blocking_time,
                    consumer_do_retry,
                    consumer_retry_count,
                    consumer_retry_blocking_time_ms,
                    consumer_max_re_retry,
                    null_handler,
                    run_with_lock_time_extender,
                    lock_time_extender_suffix,
                    lock_time_extender_add_time,
                    lock_time_extender_blocking_time,
                    lock_time_extender_replace_ttl,
                    delete_event_wait_time,
                    args=args,
                    **kwargs
                )
                return result
            
            return async_wrapper
        
        return decorator