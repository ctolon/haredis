"""Implementation of Distributed Lock-Release Algorithm for Distributed Caching Locking in Redis"""

from typing import Callable, Union, Any
import logging
import json
import asyncio
from functools import partial
import inspect

from redis.asyncio.lock import Lock

from ..client import AioHaredisSentinelClient


class HASentinelredlockManager(object):
    """## Redis Lock Release Manager Class for Distributed Caching/Locking in Redis
    This class is used to implement distributed locking in redis using stream api xread/xadd (For both asyncronous/syncronous execution).
    """

    def __init__(
        self,
        aioredis_client: AioHaredisSentinelClient,
        redis_logger: Union[logging.Logger, None] = None
        ):
        """Constructor for RedisLockReleaseManager for Redis as Sentinel.

        Args:
            aioredis_client (AioHaredisClient): AioHaredisClient Instance.
            redis_logger (logging.Logger, optional): Logger Instance. Defaults to None.
        """
        

        self.redis_logger = redis_logger        
        self.aioredis_client = aioredis_client
        
        if not self.redis_logger:
            self.redis_logger = logging.getLogger('dummy')
            self.redis_logger.setLevel(logging.CRITICAL)
            self.redis_logger.addHandler(logging.NullHandler())
            
        if self.redis_logger and not isinstance(self.redis_logger, logging.Logger):
            raise TypeError("redis_logger must be instance of logging.Logger.")
        
    async def _aio_delete_event(self, stream_key: dict, lock_key: str, event_id: str, event_info, logger=None, wait_time=10):
        """Delete event after provided time seconds for clean up for data consistency and memory usage
        
        Args:
            stream_key (dict): Name of the stream key
            lock_key (str): Name of the lock key
            event_id (str): Id of the event
            event_info (dict): Info of the event
            logger (logging.Logger): Logger instance. Defaults to None.
            wait_time (int): Wait time for lock time extender to be executed. Defaults to 10.
        """
        
        # Wait for provided seconds for delete event
        logger.info(f"Event will be deleted after {wait_time} seconds if lock is not acquired.")
        await asyncio.sleep(wait_time)
        
        # Check if lock is reacquired from another process, if yes, do not delete event otherwise delete event
        is_lock_acquired = await self.aioredis_client.client_conn.get(lock_key)
        if is_lock_acquired:
            logger.info(f"Event will not be deleted because a lock is reacquired.")
            return None
        
        # Delete event
        deleted = await self.aioredis_client.client_conn.xdel(stream_key, event_id)
        if deleted == 1:
            logger.info(f"Event deleted after produce: {event_info}")
        else:
            logger.warning(f"Event not deleted after produce: {event_info}")
            
    async def _aio_lock_time_extender(self, stream_key: str, lock: Lock, additional_time=10, wait_for_extend=10, replace_ttl: bool = True):
        """Asyncronous lock time extender for lock release manager
        
        Args:
            stream_key (str): Name of the stream key
            lock (Lock): Lock object
            additional_time (int): Additional time for lock time extender to be executed. Defaults to 10.
            wait_for_extend (int): Wait time for lock time extender to be executed. Defaults to 10.
            replace_ttl (bool): Replace ttl of the lock. Defaults to True.
        
        """
        
        # TODO add time for lock time extender
        
        streams = {stream_key: "$"}
        is_locked = await lock.locked()
        
        # While lock is acquired, call lock time extender consumer
        while is_locked:
            consume = await self.aioredis_client.client_conn.xread(streams=streams, count=1, block=5000)
            
            # Retrieve data from event
            if len(consume) > 0:
                print("consume:", consume)
                key, messages = consume[0]
                last_id, event_data = messages[0]
                data = event_data["result"]
                
                # If data is "end", lock extender will be closed
                if data == "end":
                    self.redis_logger.info("Lock Extender will be closed.")
                    await lock.extend(additional_time=additional_time, replace_ttl=replace_ttl)   
                    return None
                
            # Extend lock expire time
            self.redis_logger.info(f"Lock expire time will be extended: {additional_time} seconds")
            _ = await lock.extend(additional_time=additional_time, replace_ttl=replace_ttl)
            is_locked = await lock.locked()
            
            # If lock is released due to some reason (from another process, redis server restart etc.), raise RuntimeError
            # TODO Implement exception based decision to here
            if not is_locked:
                raise RuntimeError("[FATAL] Lock is released from another process or Redis Instance Restarted.")
                    
    async def _aiocall_consumer(self, lock_key: str, streams: dict, blocking_time_ms: int, null_handler: Any, stream_key: str):
        """Call consumer when lock is not owned by current process and if already acquired by another process

        Args:
            lock_key (str): Name of the lock key
            streams (dict): Streams to be consumed
            blocking_time_ms (int): Blocking time in milliseconds for consumers
            null_handler (Any): Null handler for empty result (it can be {}, [] or null)
            stream_key (str): Name of the stream key

        Returns:
            Any: Result of the function
        """
        
        self.redis_logger.warning(f"Lock key: {lock_key} acquire failed. Result will be tried to retrieve from consumer")
        result = await self.aioredis_client.consume_event_xread(streams=streams, lock_key=lock_key, blocking_time_ms=blocking_time_ms)
                
        if "from-event" in result.keys():
            
            # Retrieve data from event
            result = result["from-event"]
            key, messages = result[0]
            last_id, event_data = messages[0]
            data = event_data["result"]
            event_info = await self.aioredis_client.client_conn.xinfo_stream(stream_key)
            self.redis_logger.info(f"Event Received from producer: {event_info}")
            
            if isinstance(data, str) and data == "null":
                # Return null_handler
                return null_handler
            
            if isinstance(data, str) and data.startswith("RedException"):
                # Return string as RedException:<exception_string>
                return data
            
            data = json.loads(data)
            return data
        
        else:
            raise RuntimeError("An error occured while retrieving data from consumer.")
        
    async def aio_lock_release_with_stream(   
        self,
        function_name: Callable,
        lock_key: str,
        lock_extender_suffix="lock_extender",
        blocking_time_ms=5000,
        null_handler="null",
        expire_time=30,
        wait_time=10,
        additional_time=10,
        replace_ttl = True,
        *args,
        **kwargs
        ):
        """haredis distributed locking algorithm implementation in redis using stream api xread/xadd (For both syncronous/asyncronous execution)

        Args:
            function_name (Callable): Function name to be executed.
            lock_key (str): Name of the lock key.
            lock_extender_suffix (str, optional): Suffix for lock extender stream key. Defaults to "lock_extender".
            blocking_time_ms (int): Blocking time in milliseconds for consumers. Defaults to 5000.
            null_handler (Any): Null handler for empty result (it can be {}, [] or null). Defaults to "null".
            expire_time (int): Expiry time of the lock. Defaults to 30.
            wait_time (int): Wait time for lock time extender to be executed. Defaults to 10.
            additional_time (int): Additional time for lock time extender to be executed. Defaults to 10.
            replace_ttl (bool): Replace ttl of the lock. Defaults to True.

        Returns:
            Any: Result of the function
        """
        
        nullable = [{}, [], "null"]
        exception_string = None
        
        # Null handler must be empty as dict or list object or null as string
        if null_handler not in nullable:
            raise Exception("null_handler must be empty or null as string.")
        
        result = None
        exception_found = False
        
        # Define stream key and lock extender stream key based on lock key
        stream_key = f"stream:{lock_key}"
        lock_extend_stream_key = f"{stream_key}.{lock_extender_suffix}"
        streams = {stream_key: "$"}
        
        loop = asyncio.get_event_loop()
        
        # Acquire lock
        lock = await self.aioredis_client.acquire_lock(lock_key, expire_time)
        is_locked = await self.aioredis_client.is_locked(lock)
        is_owned = await self.aioredis_client.is_owned(lock)
               
        # If lock is not owned by current process, call consumer otherwise call producer         
        if is_owned:
            try:
                
                # Get Functions as partial for asyncio.gather or run_sync_async_parallel                          
                partial_main = partial(function_name, *args, **kwargs)
                partial_lock_time_extender = partial(self._aio_lock_time_extender, lock_extend_stream_key, lock, additional_time, replace_ttl)
                tasks = [partial_main, partial_lock_time_extender]
                is_main_coroutine = inspect.iscoroutinefunction(function_name)
                
                # Check if function is coroutine function, if not, run it with run_sync_async_parallel, if yes, run it with asyncio.gather
                if is_main_coroutine:
                    runner = await asyncio.gather(tasks[0](), tasks[1](), loop=loop, return_exceptions=False)
                else:
                    runner = await asyncio.gather(
                        loop.run_in_executor(None, partial_main),
                        self._aio_lock_time_extender(lock_extend_stream_key, lock, additional_time, replace_ttl),
                        return_exceptions=False
                    )
                result = runner[0]

            except Exception as e:
                exception_string = e.args[0]
                self.redis_logger.error(f"Exception: {exception_string}")
                result = exception_string
                exception_found = True
            finally:
                if exception_string:
                    print(f"Exception found {exception_string}")

                # Check if result is exception
                if exception_found:
                    self.redis_logger.error(f"Result is exception. Lock key: {lock_key} will be released. Exception: {result}")
                    raw_data = f"RedException:{result}"
                    event_data = {"result": raw_data}
                    _ = await self.aioredis_client.produce_event_xadd(stream_name=stream_key, data=event_data, maxlen=1)
                    event_info = await self.aioredis_client.client_conn.xinfo_stream(stream_key)
                    event_id = event_info["last-entry"][0]
                    self.redis_logger.info(f"Event produced to notify consumers: {event_info}")
                    await self.aioredis_client.release_lock(lock)
                    asyncio.ensure_future(self._aio_delete_event(stream_key, lock_key, event_id, event_info, self.redis_logger, wait_time), loop=loop) 
                    return raw_data
                
                # Check if result is empty
                if result is None or result == {} or result == [] or result == "null":
                    self.redis_logger.warning(f"Result is empty. Lock key: {lock_key} will be released")
                    raw_data = "null"
                    event_data = {"result": raw_data}
                    _ = await self.aioredis_client.produce_event_xadd(stream_name=stream_key, data=event_data, maxlen=1)
                    event_info = await self.aioredis_client.client_conn.xinfo_stream(stream_key)
                    event_id = event_info["last-entry"][0]
                    self.redis_logger.info(f"Event produced to notify consumers: {event_info}")
                    await self.aioredis_client.release_lock(lock)
                    asyncio.ensure_future(self._aio_delete_event(stream_key, lock_key, event_id, event_info, self.redis_logger, wait_time), loop=loop) 
                    return null_handler
                
                # If everything is ok, serialize data, produce event to consumers and finally release lock
                event_data = {"result": json.dumps(result)}
                _ = await self.aioredis_client.produce_event_xadd(stream_name=stream_key, data=event_data, maxlen=1)
                event_info = await self.aioredis_client.client_conn.xinfo_stream(stream_key)
                event_id = event_info["last-entry"][0]
                # event_data = event_info["last-entry"][1]
                self.redis_logger.info(f"Event produced to notify consumers: {event_info}")
                await self.aioredis_client.release_lock(lock)
                asyncio.ensure_future(self._aio_delete_event(stream_key, lock_key, event_id, event_info, self.redis_logger, wait_time), loop=loop) 
        else:
            result = await self._aiocall_consumer(lock_key, streams, blocking_time_ms, null_handler, stream_key)
            return result
            
        return result