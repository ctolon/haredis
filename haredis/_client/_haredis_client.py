"""HARedis Redis Client Implementation with Sync API."""
import time
from typing import Union

import redis
from redis.lock import Lock


class HaredisClient:
    """
    Redis Client Implementation with Sync API. It inherits from main redis.StrictRedis class and it has extra implementations.
    """
    def __init__(self, client_conn: Union[redis.StrictRedis, redis.Redis]):
        """Constructor method for HaredisClient class.

        Args:
            client_conn (Union[redis.StrictRedis, redis.Redis]): Redis client connection object.

        Raises:
            TypeError: If client connection is not redis.StrictRedis or redis.Redis
        """
        
        self.__client_conn = client_conn
        if type(self.__client_conn) not in (redis.StrictRedis, redis.Redis):
            raise TypeError("Client connection must be redis.StrictRedis or redis.Redis")
        
        
    @property
    def client_conn(self):
        """Getter method for client connection object.

        Returns:
            Union[redis.StrictRedis, redis.Redis]: Redis client connection object.
        """
        
        return self.__client_conn
        
    def produce_event_xadd(self, stream_name: str, data: dict, max_messages = 1, maxlen=1, stream_id="*"):
        """
        This method produces messages to a Redis Stream. Defaults to produce 1 event. If you want to produce more than 1 event,
        you can use max_messages argument. If you want to produce events infinitely, you can set max_messages to None.

        Args:
            stream_name (string): Name of the stream.
            data (dict): Data to be sent to the stream as event.
            max_messages (int, optional): Max message limit. Defaults to 1. If None, it will produce messages infinitely.
            stream_id (str, optional): Stream id. Defaults to "*". If *, it will generate unique id automatically.
            maxlen (int, optional): Max length of the stream. Defaults to 1.
            strict (bool, optional): If True, it will raise RuntimeError if producer name is not provided. Defaults to False.

        Raises:
            RuntimeError: If producer name is not provided.
            
        """
        
        # Counter for max message limit
        count = 0
                
        # Add event to stream as limited message (Defaults to 1 message to add stream).
        if max_messages:
            while count < max_messages:
                _ = self.client_conn.xadd(name=stream_name, fields=data, id=stream_id, maxlen=maxlen)

                info = self.client_conn.xinfo_stream(stream_name)
                # print(f"Produced Event Info: {info}")
                time.sleep(1)
                
                # increase count for max message limit
                count += 1 
                if count == max_messages:
                    break
        
        # Add event to stream infinitely.
        else:
            print("[WARNING]: Events will be produced infinitely. For stop producing, kill the process.")
            while True:
                _ = self.client_conn.xadd(name=stream_name, fields=data, id=stream_id)
                
                # Write event info to console.
                info = self.client_conn.xinfo_stream(stream_name)
                # print(f"Event Info: {info}")
                time.sleep(1)
                
    def consume_event_xread(self, streams: dict, lock_key: str, blocking_time_ms = 5000, count = 1):
        """This method consumes messages from a Redis Stream infinitly w/out consumer group.

        Args:
            streams (dict): {stream_name: stream_id, ...} dict. if you give '>' to stream id,
                 which means that the consumer want to receive only messages that were never delivered to any other consumer.
                 It just means, give me new messages.
            blocking_time_ms (int, optional): Blocking time. Defaults to 5000.
            count (int, optional): if set, only return this many items, beginning with the
               earliest available. Defaults to 1.
               
        Returns:
            Any: Returns consumed events as list of dicts.

        """
        
        # Timeout lazım
        # Sürekli lock'ı control etmesi lazım, lock yoksa belli bir süre dinlemesi lazım. Eğer cevap gelmezse o sürede exception fırlatmak lazım.
        
        while self.client_conn.get(lock_key) is not None:
            resp = self.client_conn.xread(
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
            
        raise Exception("No event found in stream.")
                        
    def create_consumer_group(self, stream_name: str, group_name: str, mkstream=False, id="$"):
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
            resp = self.client_conn.xgroup_create(name=stream_name, id=id, groupname=group_name, mkstream=mkstream)
            print(f"Consumer group created Status: {resp}")
        except redis.exceptions.ResponseError as e:
            print("Consumer group already exists.")
            print(f"Consumer group info: {self.client_conn.xinfo_groups(stream_name)}")
            
    def consume_event_xreadgroup(
        self,
        streams: dict,
        group_name: str,
        consumer_name: str,
        blocking_time_ms = 5000,
        count = 1,
        noack=True,
        timeout: int= None
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
              
        while True:
            resp = self.client_conn.xreadgroup(
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
                # print(f"Event catched: {resp}")
                # print(f"Event id: {last_id}")
                # print(f"Event data {data}")
                
                return resp
            
    def xtrim_with_id(self, stream_name: str, id: str):
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
        
        is_stream_exists = self.client_conn.exists(stream_name)
        if not is_stream_exists:
            resp = self.client_conn.xtrim(name=stream_name, minid=id)
            return resp
        return "WARNING: Stream does not exists..."
    
    def produce_event_publish(self, pubsub_channel: str, message: str):
        """
        Send events to pub-sub channel.

        Args:
            pubsub_channel (str): Pub-Sub channel name which will be used to publish events
            message (str): Event to publish to subscribers channel

        """

        event = self.client_conn.publish(channel=pubsub_channel, message=message)
        # print(f"event {message} consumed by number of {event} consumers")
        
    def consume_event_subscriber(self, pubsub_channel: str):
        """
        Firstly it subscribes to pubsub_channel and then when it receives an event, it consumes it.

        Args:
            pubsub_channel (str): Name of the pub-sub channel to subscribe which was created by publisher

        """
        
        ps = self.client_conn.pubsub()
        ps.subscribe(pubsub_channel)
        for raw_message in ps.listen():
            if raw_message['type'] == 'message':
                result = raw_message['data']
                return result
                # yield result

    def acquire_lock(self, lock_key, expire_time=40):
        """This function allows to assign a lock to a key for distrubuted caching.

        Args:
            lock_key (string): Name of the Lock key.
            expire_time (int, optional): Expire time for lock key. Defaults to 40.

        Returns:
            Lock: Redis Lock object.
        """
             
        lock = self.client_conn.lock(name=lock_key, timeout=expire_time, blocking_timeout=5)
        acquire = lock.acquire()
        return lock
    
    def is_locked(self, redis_lock: Lock):
        """Check if lock object is locked or not."""       
        
        if redis_lock.locked():
            return True
        return False
    
    def is_owned(self, lock: Lock):
        """Check if lock object is owned by current process or not."""
        
        if lock.owned():
            return True
        return False
    
    def release_lock(self, redis_lock: Lock):
        """Release lock object."""
        
        if self.client_conn.get(redis_lock.name) is not None:
            _ = redis_lock.release()
        else:
            print(f"[FATAL]: Redis Lock does not exists! Possibly it is expired. Increase expire time for Lock.")