"""Implementation of Distributed Lock-Release Algorithm for Distributed Caching Locking in Redis"""

from typing import Callable, Union, Any
import logging
import json
import asyncio
from functools import partial

import redis
from redis import asyncio as aioredis
from redis.asyncio.lock import Lock

from .._client import AioHaredisClient, HaredisClient


class _BaseLockRelaseManager(object):
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
        
        self.__aioharedis_client = AioHaredisClient(client_conn=aioredis_client)
        self.__haredis_client = HaredisClient(client_conn=redis_client)
        self.__redis_logger = redis_logger        

        
        if not self.__redis_logger:
            self.__redis_logger = logging.getLogger('dummy')
            self.__redis_logger.setLevel(logging.CRITICAL)
            self.__redis_logger.addHandler(logging.NullHandler())
            
        if self.redis_logger and not isinstance(self.__redis_logger, logging.Logger):
            raise TypeError("redis_logger must be instance of logging.Logger.")
        
    @property
    def aioharedis_client(self):
        return self.__aioharedis_client
    
    @property
    def haredis_client(self):
        return self.__haredis_client
    
    @property
    def redis_logger(self):
        return self.__redis_logger
        
    async def aio_delete_event(
        self,
        consumer_stream_key: dict,
        lock_key: str,
        event_id: str,
        event_info: dict,
        delete_event_wait_time=10
        ):
        """Delete event after provided time seconds for clean up for data consistency and memory usage
        
        Args:
            consumer_stream_key (dict): Name of the stream key
            lock_key (str): Name of the lock key
            event_id (str): Id of the event
            event_info (dict): Info of the event
            delete_event_wait_time (int): Wait time for delete event operation in seconds. Defaults to 10.
        """
        
        if not (isinstance(delete_event_wait_time, int) or isinstance(delete_event_wait_time, float)):
            raise TypeError("delete_event_wait_time must be integer or float.")
        
        if delete_event_wait_time < 1:
            raise ValueError("delete_event_wait_time must be greater than 1 or must be equals to 1.")
        
        # Wait for provided seconds for delete event
        self.redis_logger.info("Event will be deleted after {delete_event_wait_time} seconds."
                               .format(delete_event_wait_time=delete_event_wait_time))
        await asyncio.sleep(delete_event_wait_time)
        
        # TODO event id's are unique for each event. So, we can check if event is deleted or not by checking event id's
        # self.redis_logger.info(f"Event will be deleted after {delete_event_wait_time} seconds if lock is not acquired.")
        # while await self.aioharedis_client.client_conn.get(lock_key):
        #     self.redis_logger.info(f"Event will not be deleted because a lock is reacquired. Waiting for 5 more seconds.")
        #     await asyncio.sleep(5)
        #     return None
        
        # Delete event
        _ = await self.aioharedis_client.client_conn.xdel(consumer_stream_key, event_id)
        
        # Check if event is deleted
        if _ == 1:
            self.redis_logger.info("Event deleted after produce: {event_info}"
                                   .format(event_info=event_info))
        else:
            self.redis_logger.warning("Event not deleted after produce: {event_info}"
                                      .format(event_info=event_info))
            
    async def warn_aio_lock_time_extender(
        self,
        lock_time_extender_suffix,
        lock_time_extender_add_time,
        lock_time_extender_blocking_time,
        lock_time_extender_replace_ttl, 
        ):
        if lock_time_extender_suffix is not None:
            self.redis_logger.warning("lock_time_extender_suffix will be ignored because run_with_lock_time_extender is False.")
        
        if lock_time_extender_add_time is not None:
            self.redis_logger.warning("lock_time_extender_add_time will be ignored because run_with_lock_time_extender is False.")
            
        if lock_time_extender_blocking_time is not None:
            self.redis_logger.warning("lock_time_extender_blocking_time will be ignored because run_with_lock_time_extender is False.")
            
        if lock_time_extender_replace_ttl is not None:
            self.redis_logger.warning("lock_time_extender_replace_ttl will be ignored because run_with_lock_time_extender is False.")
            
    async def aio_lock_time_extender(
        self,
        lock: Lock,
        consumer_stream_key: str,
        lock_time_extender_suffix: str,
        lock_time_extender_add_time=10,
        lock_time_extender_blocking_time=5000,
        lock_time_extender_replace_ttl: bool = True
        ):
        """Asyncronous lock time extender for lock release manager
        
        Args:
            lock (Lock): Lock object
            consumer_stream_key (str): Name of the stream key
            lock_time_extender_suffix (str): Suffix for lock extender stream key
            lock_time_extender_add_time (int): Additional time for lock time extender to be executed in seconds. Defaults to 10.
            lock_time_extender_blocking_time (int): Blocking time in milliseconds for lock time extender. Defaults to 5.
            lock_time_extender_replace_ttl (bool): Replace ttl of the lock. Defaults to True. If False, expire will be used instead of ttl.
        
        """
        
        # Type check if run with lock time extender is True
        if not (isinstance(lock_time_extender_add_time, int) or isinstance(lock_time_extender_add_time, float)):
            type_lock_time_extender_add_time = type(lock_time_extender_add_time)
            raise TypeError("lock_time_extender_add_time must be integer or float. Found: {type_lock_time_extender_add_time}"
                            .format(type_lock_time_extender_add_time=type_lock_time_extender_add_time))
        
        if not isinstance(lock_time_extender_blocking_time, int):
            type_lock_time_extender_blocking_time = type(lock_time_extender_blocking_time)
            raise TypeError("lock_time_extender_blocking_time must be integer. Found: {type_lock_time_extender_blocking_time}"
                            .format(type_lock_time_extender_blocking_time=type_lock_time_extender_blocking_time))
        
        if not isinstance(lock_time_extender_replace_ttl, bool):
            type_lock_time_extender_replace_ttl = type(lock_time_extender_replace_ttl)
            raise TypeError("lock_time_extender_replace_ttl must be boolean. Found: {type_lock_time_extender_replace_ttl}"
                            .format(type_lock_time_extender_replace_ttl=type_lock_time_extender_replace_ttl))
    
        if lock_time_extender_add_time < 1:
            raise ValueError("lock_time_extender_add_time must be greater than 1. Found: {lock_time_extender_add_time}"
                             .format(lock_time_extender_add_time=lock_time_extender_add_time))
        
        if lock_time_extender_blocking_time < 1:
            raise ValueError("lock_time_extender_blocking_time must be greater than 1.")
        
        if lock_time_extender_blocking_time > lock_time_extender_add_time * 1000:
            raise ValueError("lock_time_extender_blocking_time must be less than lock_time_extender_add_time.")

 
        lock_extend_stream_key = "{consumer_stream_key}.{lock_time_extender_suffix}".format(
            consumer_stream_key=consumer_stream_key,
            lock_time_extender_suffix=lock_time_extender_suffix
            )
        streams = {lock_extend_stream_key: "$"}
        is_locked = await lock.locked()
        
        # TODO maybe implement lock token based lock time extender here and delete execute_with layers.
        
        # While lock is acquired, call lock time extender consumer
        while is_locked:
            consume = await self.aioharedis_client.client_conn.xread(streams=streams, count=1, block=lock_time_extender_blocking_time)
            
            # Retrieve data from event
            if len(consume) > 0:
                self.redis_logger.debug("Lock Extender: Event Received from producer: {consume}"
                                        .format(consume=consume))
                key, messages = consume[0]
                last_id, event_data = messages[0]
                data = event_data["result"]
                
                # If data is "end", lock extender will be closed
                if data == "end":
                    self.redis_logger.info("Lock Extender will be closed.")
                    _ = await self.aioharedis_client.client_conn.xdel(lock_extend_stream_key, last_id)
                    self.redis_logger.debug("Lock Extender event deleted: {last_id}"
                                            .format(last_id=last_id))
                    await lock.extend(
                        additional_time=lock_time_extender_add_time,
                        replace_ttl=lock_time_extender_replace_ttl
                        )   
                    return None
                
            # Extend lock expire time
            if lock_time_extender_replace_ttl:
                self.redis_logger.info("Lock expire time will be extended w/ttl: {lock_time_extender_add_time} seconds"
                                       .format(lock_time_extender_add_time=lock_time_extender_add_time))
            else:
                self.redis_logger.info("Lock expire time will be extended w/expire: {lock_time_extender_add_time} seconds"
                                       .format(lock_time_extender_add_time=lock_time_extender_add_time))
                
            _ = await lock.extend(
                additional_time=lock_time_extender_add_time,
                replace_ttl=lock_time_extender_replace_ttl
                )   

            # If lock is released due to some reason (from another process, redis server restart etc.), raise RuntimeError
            is_locked = await lock.locked()
            if not is_locked:
                raise RuntimeError("[FATAL] Redis Lock is released!")
            
    async def execute_asyncfunc_with_close_lock_extender(
        self,
        aioharedis_client: AioHaredisClient,
        func: Callable,
        lock_key: str,
        lock_time_extender_suffix: str,
        redis_logger: Union[logging.Logger, None],
        args,
        **kwargs
        ) -> Any:
        """Execute asyncronous function with close lock extender for finish lock extender after async main function execution.

        Args:
            aioharedis_client (AioHaredisClient): AioHaredisClient Instance
            func (Callable): Function name to be executed.
            lock_key (str): Name of the lock key
            lock_time_extender_suffix (str): Suffix for lock extender stream key
            aioharedis_client (AioHaredisClient): AioHaredisClient Instance
            redis_logger (Union[logging.Logger, None], optional): Logger Instance. Defaults to None.
            
        Returns:
            Any: Result of the function
        """
        
        result = await func(*args, **kwargs)
        
        stream_key = "stream:{lock_key}.{lock_time_extender_suffix}".format(lock_key=lock_key, lock_time_extender_suffix=lock_time_extender_suffix)
        end_data = {"result": "end"}
                            
        await aioharedis_client.client_conn.xadd(stream_key, end_data, maxlen=1)
        redis_logger.info("Lock extender closer event sent from the main function.")
            
        return result
    
    def execute_func_with_close_lock_extender(
        self,
        haredis_client: AioHaredisClient,
        func: Callable,
        lock_key: str,
        lock_time_extender_suffix: str,
        redis_logger,
        args,
        **kwargs
        ) -> Any:
        """Execute asyncronous function with close lock extender for finish lock extender after sync main function execution.

        Args:
            aioharedis_client (AioHaredisClient): AioHaredisClient Instance
            func (Callable): Function name to be executed.
            lock_key (str): Name of the lock key
            lock_time_extender_suffix (str): Suffix for lock extender stream key
            aioharedis_client (AioHaredisClient): AioHaredisClient Instance
            redis_logger (Union[logging.Logger, None], optional): Logger Instance. Defaults to None.
            
        Returns:
            Any: Result of the function
        """
        
        result = func(*args, **kwargs)
        
        stream_key = "stream:{lock_key}.{lock_time_extender_suffix}".format(lock_key=lock_key, lock_time_extender_suffix=lock_time_extender_suffix)
        end_data = {"result": "end"}
                          
        haredis_client.client_conn.xadd(stream_key, end_data, maxlen=1)
        redis_logger.info("Lock extender closer event sent from the main function.")
            
        return result
    
    async def partial_main_selector(
        self,
        func: Callable,
        lock_key: str,
        lock_time_extender_suffix: str,
        is_main_coroutine: bool,
        run_with_lock_time_extender: bool,
        args,
        kwargs,
    ):
        """Select partial main function based on is_main_coroutine parameter

        Args:
            func (Callable): Function name to be executed.
            lock_key (str): Name of the lock key.
            lock_time_extender_suffix (str): Suffix for lock extender stream key.
            is_main_coroutine (bool): If True, execute asyncronous function, if False, execute syncronous function.
            run_with_lock_time_extender (bool): If True, lock time extender will be executed for High Availability.
            args (tuple): args
            kwargs (dict): kwargs
            
        Returns:
            Partial main function.
        """
        
        # TODO optimize this function
        
        if run_with_lock_time_extender:
            partial_main = partial(
                self.execute_func_with_close_lock_extender,
                self.haredis_client,
                func,
                lock_key,
                lock_time_extender_suffix,
                self.redis_logger,
                args,
                **kwargs
                )     
            if is_main_coroutine:
                partial_main = partial(
                    self.execute_asyncfunc_with_close_lock_extender,
                    self.aioharedis_client,
                    func,
                    lock_key,
                    lock_time_extender_suffix,
                    self.redis_logger,
                    args,
                    **kwargs
                    )
        else:
            partial_main = partial(
                func,
                args,
                **kwargs
                )
                
        return partial_main
                    
    async def aiocall_consumer(
        self,
        lock_key: str,
        streams: dict,
        consumer_blocking_time: int,
        null_handler: Any,
        consumer_stream_key: str,
        consumer_do_retry: bool,
        consumer_retry_count: int,
        consumer_retry_blocking_time_ms: int,
        consumer_max_re_retry: int
        ):
        """Call consumer when lock is not owned by current process and if already acquired by another process

        Args:
            lock_key (str): Name of the lock key.
            streams (dict): Streams to be consumed.
            consumer_blocking_time (int): Blocking time in milliseconds for consumers.
            null_handler (Any): Null handler for empty result (it can be {}, [] or null).
            consumer_stream_key (str): Name of the stream key.
            consumer_do_retry (bool): If True, consumer will be retried, if lock released.
            consumer_retry_count (int): Retry count for consumer.
            consumer_retry_blocking_time_ms (int): Blocking time in milliseconds for consumer retry.
            consumer_max_re_retry (int): Max re-retry count for consumer.

        Returns:
            Any: Result of the function
        """
        
        if not isinstance(consumer_blocking_time, int):
            raise TypeError("consumer_blocking_time must be integer.")
        
        if consumer_blocking_time < 0:
            raise ValueError("consumer_blocking_time must be greater than 0.")
                
        self.redis_logger.debug("Lock key: {lock_key} acquire failed. Result will be tried to retrieve from consumer".format(lock_key=lock_key))
        result = await self.aioharedis_client.consume_event_xread(
            streams=streams,
            lock_key=lock_key,
            blocking_time_ms=consumer_blocking_time,
            do_retry=consumer_do_retry,
            retry_count=consumer_retry_count,
            retry_blocking_time_ms=consumer_retry_blocking_time_ms,
            max_re_retry=consumer_max_re_retry
            )
                
        if "from-event" in result.keys():
            
            # Retrieve data from event
            result = result["from-event"]
            key, messages = result[0]
            last_id, event_data = messages[0]
            data = event_data["result"]
            event_info = await self.aioharedis_client.client_conn.xinfo_stream(consumer_stream_key)
            self.redis_logger.info("Event Received from producer: {event_info}".format(event_info=event_info))
            
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
        
    async def lock_key_generator(
        self,
        keys_to_lock: tuple,
        args: tuple,
        kwargs: dict,
        lock_key_prefix: Union[str, None] = None,
        ) -> str:
        """This function generates lock key based on positional and keyword arguments

        Args:
            keys_to_lock (tuple): Keys to be locked
            args (tuple): function positional arguments
            kwargs (dict): function keyword arguments
            redis_logger (Union[logging.Logger, None], optional): Logger Instance. Defaults to None.
            lock_key_prefix (Union[str, None], optional): Prefix for lock key. Defaults to None.

        Raises:
            ValueError: If positional or keyword arguments are missing, raise ValueError

        Returns:
            str: lock key
        """
        
        if not isinstance(keys_to_lock, tuple):
            raise TypeError("keys_to_lock must be tuple.")
        
        if lock_key_prefix and not isinstance(lock_key_prefix, str):
            raise TypeError("lock_key_prefix must be string or None.")
        
        lock_list = []
        idx = 0
        lock_key_suffix = ".lock"
                
        keys_to_lock_args = [item for item in keys_to_lock if item in args]
        keys_to_lock_kwargs = [item for item in keys_to_lock if item in list(kwargs.keys())]
        self.redis_logger.debug("keys_to_lock_args: {keys_to_lock_args}".format(keys_to_lock_args=keys_to_lock_args))
        self.redis_logger.debug("keys_to_lock_kwargs: {keys_to_lock_kwargs}".format(keys_to_lock_kwargs=keys_to_lock_kwargs))
        
        # Add positional arguments to lock key
        if len(keys_to_lock_args) > 0:
            for idx, lock_name in enumerate(keys_to_lock_args):
                if args.get(lock_name) is None:
                    param = "null"
                else:
                    param = str(args.get(lock_name))
                fmt_str = str(idx+1) + ":" + param
                lock_key = "arg" +  fmt_str
                lock_list.append(lock_key)
        
        # Add keyword arguments to lock key
        if len(keys_to_lock_kwargs) > 0:
            for idx ,lock_name in enumerate(keys_to_lock):
                if kwargs.get(lock_name) is None:
                    param = "null"
                else:
                    param = str(kwargs.get(lock_name))
                fmt_str = str(idx+1) + ":" + param
                lock_key = "param" + fmt_str
                lock_list.append(lock_key)
                
        # If no positional or keyword arguments are provided, raise ValueError
        if len(lock_list) == 0:
            raise ValueError("No lock key parameter is provided.")
        
        if lock_key_prefix:
            lock_key_suffix = ".{lock_key_prefix}{lock_key_suffix}".format(lock_key_prefix=lock_key_prefix, lock_key_suffix=lock_key_suffix)
        
        lock_key = "&".join(lock_list) + lock_key_suffix
        return lock_key
    
    async def get_result_from_cache(self, response_cache: int, cache_key: str, extend_cache_time: bool = None):
        """_summary_

        Args:
            response_cache (int): Response cache time in seconds 
            cache_key (str): Cache key
            extend_cache_time (bool): If True, cache time will be extended for response_cache seconds. Defaults to None.

        Raises:
            TypeError: If response_cache is not integer, raise
            TypeError: If extend_cache_time is not boolean or None Type, raise

        Returns:
            Any: Result from cacahe
        """
        
        if not isinstance(response_cache, int):
            raise TypeError("response_cache must be integer.")
        
        # if not isinstance(extend_cache_time, bool) or extend_cache_time is not None:
        #     raise TypeError("extend_cache_time must be boolean or None Type.")
        
        self.redis_logger.debug("Result will be get from redis cache if exists.")
        cache_result = await self.aioharedis_client.client_conn.get(name=cache_key)
        if cache_result:
            self.redis_logger.debug("Result found in redis cache.")
            cache_result = json.loads(cache_result)
            if extend_cache_time:
                self.redis_logger.debug("Cache time will be extended for {response_cache} seconds.".format(response_cache=response_cache))
                await self.aioharedis_client.client_conn.expire(name=cache_key, time=response_cache)
            return cache_result
        self.redis_logger.debug("Result not found in redis cache.")
        
    async def set_result_to_cache(self, cache_key: str, response_cache: int, result: Any):
        """Set result to redis cache

        Args:
            cache_key (str): Cache key
            response_cache (int): Response cache time in seconds
            result (Any): Result to be cached

        Raises:
            TypeError: If response_cache is not integer, raise TypeError
        """
        
        if not isinstance(response_cache, int):
            raise TypeError("response_cache must be integer.")

        if response_cache:
            self.redis_logger.debug("Response will be cached for {response_cache} seconds.".format(response_cache=response_cache))
            
            # If result is dict, try to convert it to json
             # if isinstance(result, dict):
             #     try:
             #         result = json.dumps(result)
             #     except Exception as e:
             #         pass
                
                
            cache_set_status = await self.aioharedis_client.client_conn.set(cache_key, result, ex=response_cache)
            self.redis_logger.debug("Set Cache Response status: {cache_set_status}".format(cache_set_status=cache_set_status))
        
