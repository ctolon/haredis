"""haredis AioRedis Sentinel Client Implementation."""
import asyncio

from redis.asyncio import sentinel
from redis import asyncio as aioredis
from redis.asyncio.lock import Lock


class AioHaredisSentinelClient(sentinel.SentinelConnectionPool):
    """
    ...
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        try:
            self.sentinel_conn = sentinel.Sentinel(connection_pool=self)
        except aioredis.ConnectionError as e:
            print(f"Error connecting to Redis: {e}")

    async def connect_client(self, *args, **kwargs):
        """Main connection method. It creates Redis Instance."""

        try:
            host, port = await self.sentinel_conn.discover_master(kwargs.get("service_name"))
            self.client_conn = aioredis.Redis(*args, **kwargs)
            await self.client_conn.initialize()
            _ = await self.client_conn.ping()
            print(f"Ping Response on Redis Client: {_}")
            print("Connected to Redis Sentinel with async API.")
        except Exception as e:
            print(f"Error connecting to Redis Sentinel: {e}")

    async def close_client(self):
        """Close AioRedis connection."""
        await self.disconnect(inuse_connections=True)
        await self.client_conn.close(self)

        print("Disconnected from Redis.")
                        
    async def get_aioredis_sentinel_client(self):
        """Get AioRedis Sentinel Client as Connection Pool."""
        return self

    async def get_native_aioredis(self) -> aioredis.Redis:
        """Get Native AioRedis Instance."""
        
        return self.client_conn

    async def execute(self, command, *args, **kwargs):
        """Execute any AioRedis command with native API."""
        
        try:
            result = await self.client_conn.execute_command(command, *args, **kwargs)
            return result
        except aioredis.RedisError as e:
            print(f"Redis command error: {e} - {command} {args} {kwargs}")
            return None
        
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
            print("[WARNING]: Events will be produced infinitely. For stop producing, kill the process.")
            while True:
                _ = await self.client_conn.xadd(name=stream_name, fields=data, id=stream_id)
                
                # Write event info to console.
                info = await self.client_conn.xinfo_stream(stream_name)
                # print(f"Event Info: {info}")
                await asyncio.sleep(1)
            
    async def consume_event_xread(self, streams: dict, lock_key: str, blocking_time_ms = 5000, count = 1):
        """This method consumes messages from a Redis Stream infinitly w/out consumer group.

        Args:
            streams (dict): {stream_name: stream_id, ...} dict. if you give '>' to stream id,
                 which means that the consumer want to receive only messages that were never delivered to any other consumer.
                 It just means, give me new messages.
            lock_key (str): Name of the lock key.
            blocking_time_ms (int, optional): Blocking time. Defaults to 5000.
            count (int, optional): if set, only return this many items, beginning with the
               earliest available. Defaults to 1.
               
        Returns:
            Any: Returns consumed events as list of dicts.

        """
        
        lock_key_status = await self.client_conn.get(lock_key)
        while lock_key_status is not None:
            resp = await self.client_conn.xread(
                streams=streams,
                count=count,
                block=blocking_time_ms
            )
            
            if resp:
                key, messages = resp[0]
                last_id, data = messages[0]
                # print(f"Event id: {last_id}")
                # print(f"Event data {data}")           
                return {"from-event": resp}
            
            lock_key_status = await self.client_conn.get(lock_key)
         
        raise Exception("Lock is not acquired. Can not consume events from stream.")   
             
                
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
            print(f"Consumer group created Status: {resp}")
        except Exception as e:
            info = await self.client_conn.xinfo_groups(stream_name)
            print(f"Consumer group info: {info}")

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
        
        lock = self.client_conn.lock(name=lock_key, timeout=expire_time, blocking_timeout=5)
        is_acquired = await lock.acquire()
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
            print(f"[FATAL]: Redis Lock does not exists! Possibly it is expired. Increase expire time for Lock.")
