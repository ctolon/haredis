"""HARedis AioRedis Client Implementation."""
import logging
import asyncio
from typing import Union

from redis import asyncio as aioredis
from redis.asyncio.lock import Lock


class AioHaredisClient:
    """
    haredis client class for redis async api.
    """
    
    def __init__(self, client_conn: Union[aioredis.StrictRedis, aioredis.Redis], redis_logger: logging.Logger = None):
        """Constructor method for HaredisClient class.

        Args:
            client_conn (Union[redis.StrictRedis, redis.Redis]): Redis client connection object.

        Raises:
            TypeError: If client connection is not redis.StrictRedis or redis.Redis
        """
        
        self.__client_conn = client_conn
        if type(self.__client_conn) not in (aioredis.StrictRedis, aioredis.Redis):
            raise TypeError("Client connection must be redis.StrictRedis or redis.Redis")
        
        self.__redis_logger = redis_logger        

        
        if not self.__redis_logger:
            self.__redis_logger = logging.getLogger('dummy')
            self.__redis_logger.setLevel(logging.CRITICAL)
            self.__redis_logger.addHandler(logging.NullHandler())
            
        if self.__redis_logger and not isinstance(self.__redis_logger, logging.Logger):
            raise TypeError("redis_logger must be instance of logging.Logger.")
        
        
    @property
    def client_conn(self):
        """Getter method for client connection object.

        Returns:
            Union[redis.StrictRedis, redis.Redis]: Redis client connection object.
        """
        
        return self.__client_conn
    
    @property
    def redis_logger(self):
        """Getter method for redis logger.

        Returns:
            logging.Logger: Redis logger.
        """
        
        return self.__redis_logger
                
    # Producer - Consumer Implementation for streams API
    async def produce_event_xadd(self, stream_name: str, data: dict, max_messages = 1, maxlen=1, stream_id="*"):
        """
        This method produces messages to a Redis Stream. Defaults to produce 1 event. If you want to produce more than 1 event,
        you can use max_messages argument. If you want to produce events infinitely, you can set max_messages to None.

        Args:
            stream_name (string): Name of the stream.
            data (dict): Data to be sent to the stream as event.
            max_messages (int, optional): Max message limit. Defaults to 1. If None, it will produce messages infinitely.
            stream_id (str, optional): Stream id. Defaults to "*". If *, it will generate unique id automatically.

        Raises:
            RuntimeError: If producer name is not provided when strict is true.
            
        """
        
        # Counter for max message limit
        count = 0
                        
        # Add event to stream as limited message (Defaults to 1 message to add stream).
        if max_messages:
            while count < max_messages:
                _ = await self.client_conn.xadd(name=stream_name, fields=data, id=stream_id, maxlen=maxlen)

                info = await self.client_conn.xinfo_stream(stream_name)
                # print(f"Produced Event Info: {info}")
                await asyncio.sleep(1)
                
                # increase count for max message limit
                count += 1 
                if count == max_messages:
                    break
        
        # Add event to stream infinitely.
        else:
            self.redis_logger.warning("Events will be produced infinitely. For stop producing, kill the process.")
            while True:
                _ = await self.client_conn.xadd(name=stream_name, fields=data, id=stream_id)
                
                # Write event info to console.
                info = await self.client_conn.xinfo_stream(stream_name)
                # print(f"Event Info: {info}")
                await asyncio.sleep(1)
            
    async def consume_event_xread(
        self,
        streams: dict,
        lock_key: str,
        blocking_time_ms = 5000,
        count = 1,
        do_retry = True,
        retry_count = 5,
        retry_blocking_time_ms = 2 * 1000,
        max_re_retry = 2
        
        ):
        """This method consumes messages from a Redis Stream infinitly w/out consumer group.

        Args:
            streams (dict): {stream_name: stream_id, ...} dict. if you give '>' to stream id,
                 which means that the consumer want to receive only messages that were never delivered to any other consumer.
                 It just means, give me new messages.
            lock_key (str): Name of the lock key.
            blocking_time_ms (int, optional): Blocking time. Defaults to 5000.
            count (int, optional): if set, only return this many items, beginning with the
               earliest available. Defaults to 1.
            do_retry (bool, optional): If True, it will retry to acquire lock. Defaults to True.
            retry_count (int, optional): Retry count. Defaults to 5.
            retry_blocking_time_ms (int, optional): Retry blocking time in miliseconds. Defaults to 2 * 1000.
            max_re_retry (int, optional): Max re-retry count for lock release problem. Defaults to 2.
               
        Returns:
            Any: Returns consumed events as list of dicts.

        """
        
        re_retry_counter = 0
        
        while True:
            
            resp = await self.client_conn.xread(
                streams=streams,
                count=count,
                block=blocking_time_ms
            )
        
            if resp:
                key, messages = resp[0]
                # last_id, data = messages[0]           
                return {"from-event": resp}
            
            lock_key_status = await self.client_conn.get(lock_key)
                        
            if lock_key_status is None:
                
                # If max_retry is reached, raise exception
                if re_retry_counter == max_re_retry:
                    raise Exception("Max re-retry count reached for Consumer.")
                
                # If not do_retry when lock released, raise exception directly
                if not do_retry:
                    raise Exception("Redis Lock is released! Consumer can't be consume events from stream.")
                
                self.redis_logger.warning("Lock is not acquired or released! Consumer will retry consume events from stream again in {retry_count} \
                    times with {retry_blocking_time_ms} seconds blocking..".format(retry_count=retry_count, retry_blocking_time_ms=retry_blocking_time_ms))
                
                # Wait for retry_blocking_time_ms seconds
                for i in range(retry_count+1):
                    i_plus_one = i + 1
                    if i == retry_count:
                        raise Exception("Max retry count reached for Consumer.")
                    
                    self.redis_logger.info("Retrying to get redis lock: {i_plus_one}.time".format(i_plus_one=i_plus_one))
                    
                    # Wait for retry_blocking_time_ms seconds
                    await asyncio.sleep({retry_blocking_time_ms}/1000)
                    
                    # Query lock key status again for retry, if lock key is reacquired, break the loop
                    # And go to the beginning of the while loop after incrementing retry_counter
                    lock_key_status = await self.client_conn.get(lock_key)
                    if lock_key_status:

                        self.redis_logger.info("New redis lock obtained after retrying {i_plus_one} time. Consumer will be retry to consume events from stream.".format(i_plus_one=i_plus_one))
                        re_retry_counter = re_retry_counter + 1
                        break
             
    async def create_consumer_group(self, stream_name: str, group_name: str, mkstream=False, id="$"):
        """
        This method allows to creates a consumer group for a stream.
        You can create multiple consumer groups for a stream.
        Also best practice is to create consumer group before producing events to stream.
        You can create consumer group with mkstream=True to create stream if not exists.
        In FastAPI, you can create consumer groups in startup event.

        Args:
            stream_name (string): Name of the stream. 
            id (string): Stream id. Defaults to "$". If "$", it will try to get last id of the stream.
            group_name (string): Name of the consumer group.
        """
                
        try:
            resp = await self.client_conn.xgroup_create(name=stream_name, id=id, groupname=group_name, mkstream=mkstream)
            self.redis_logger.info("Consumer group created Status: {resp}".format(resp=resp))
        except Exception as e:
            self.redis_logger.warning("Consumer group already exists. {e}".format(e=e))
            info = await self.client_conn.xinfo_groups(stream_name)
            self.redis_logger.info("Consumer group info: {info}".format(info=info))

        # await self.client_conn.xgroup_setid(stream_key=stream_key, groupname=group_name, id=0) 

    async def consume_event_xreadgroup(
        self,
        streams: dict,
        group_name: str,
        consumer_name: str,
        blocking_time_ms = 5000,
        count = 1,
        noack=True,
        ):
        """
        This method consumes messages from a Redis Stream infinitly as consumer group.
        If you want to consume events as consumer group, you must create consumer group first.
        Also you should delete events from stream after consuming them. Otherwise memory will be full in redis.
        You can delete events from stream with xtrim or xdel commands.

        Args:
            streams (dict): {stream_name: stream_id, ...} dict. if you give '>' to stream id,
                 which means that the consumer want to receive only messages that were never delivered to any other consumer.
                 It just means, give me new messages.
            group_name (str): Name of the consumer group
            consumer_name (str): Name of the requesting consumer.
            blocking_time_ms (int, optional): Blocking time. Defaults to 5000.
            count (int, optional): if set, only return this many items, beginning with the
               earliest available. Defaults to 1.
            noack (bool, optional): If True, it will not add events to pending list. Defaults to True.

        Returns:
            Any: Returns consumed events as list of dicts.
        """
        
        # While true lock key exist ile değişecek, Timeout policyler belirlenecek  
        while True:
            resp = await self.client_conn.xreadgroup(
                groupname=group_name,
                consumername=consumer_name,
                streams=streams,
                block=blocking_time_ms,
                count=count,
                noack=noack
                )
                        
            if resp:
                key, messages = resp[0]
                last_id, data = messages[0]
                # print(f"Event id: {last_id}")
                # print(f"Event data {data}")
                return resp
            
    async def get_last_stream_id(self, stream_name: str):
        """Get Last Stream Id from stream."""
        
        last_id = await self.client_conn.xrevrange(stream_name, count=1)
        return last_id[0][0]
                           
    async def xtrim_with_id(self, stream_name: str, id: str):
        """This method allows to delete events from stream w/ trim.
        After deleting events from stream, you can not consume them again.
        For this reason, you should use this method carefully.
        This function provides to free memory for redis.

        Args:
            stream_name (str): Name of the stream.
            id (str): Name of the event id.

        Returns:
            Any: Deletion response or Warning message if stream not exists.
        """
                
        is_stream_exists = await self.client_conn.exists(stream_name)
        if not is_stream_exists:
            resp = await self.client_conn.execute_command('XTRIM', stream_name, 'MINID', id)
            return resp
        return "WARNING: Stream does not exists..."
                    
    async def produce_event_publish(self, pubsub_channel: str, message: str):
        """
        Send events to pub-sub channel.

        Args:
            pubsub_channel (str): Pub-Sub channel name which will be used to publish events
            message (str): Event to publish to subscribers channel

        """
        
        event = await self.client_conn.publish(channel=pubsub_channel, message=message)
        # print(f"event {message} consumed by number of {event} consumers")
        
                
    async def consume_event_subscriber(self, pubsub_channel: str):
        """
        Firstly it subscribes to pubsub_channel and then when it receives an event, it consumes it.

        Args:
            pubsub_channel (str): Name of the pub-sub channel to subscribe which was created by publisher

        Returns:
            Event: Event from publisher.
        """
                
        ps = self.client_conn.pubsub()
        await ps.subscribe(pubsub_channel)
        async for raw_message in ps.listen():
            if raw_message['type'] == 'message':
                result = raw_message['data']
                return result
                # yield result
        
    async def acquire_lock(self, lock_key: str, expire_time=40):
        """This function allows to assign a lock to a key for distrubuted caching.

        Args:
            lock_key (_type_): Name of the Lock key.
            expire_time (int, optional): Expire time for lock key. Defaults to 40.

        Returns:
            Lock: Redis Lock object.
        """
        
        lock = self.client_conn.lock(name=lock_key, timeout=expire_time, blocking_timeout=5, thread_local=True)
        is_acquired = await lock.acquire(blocking=True, blocking_timeout=0.01)
        return lock
    
    async def is_locked(self, redis_lock: Lock):
        """Check if lock object is locked or not."""
                
        if await redis_lock.locked():
            return True
        return False
    
    async def is_owned(self, lock: Lock):
        """Check if lock object is owned or not."""
        
        if await lock.owned():
            return True
        return False
    
    async def release_lock(self, redis_lock: Lock):
        """Try to release lock object."""
        
        if await self.client_conn.get(redis_lock.name) is not None:
            _ = await redis_lock.release()
        else:
            self.redis_logger.warning("Redis Lock does not exists! Possibly it is expired. Increase expire time for Lock.")
