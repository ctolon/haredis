# haredis: Python Extension for redis with Abstractions

`haredis` is a Python module integrating redis/aioredis
lock-release algorithms with a simple way in High Availability.

It aims to provide simple and efficient solutions to lock-release problems
with streaming API.

`NOTE:` This project is under development. It isn't full ready for production use.

* `Author`: Cevat Batuhan Tolon
* `Contact`: cevat.batuhan.tolon@cern.ch

## Requirements

* Python 3.6+
* redis>=4

## Installation

```bash
pip install haredis
```

### Basic Usage With FastAPI

This example shows how to use haredis with Redis Standalone setup w/FastAPI.

* First, you need to create a Redis Standalone instance. You can use Docker for this purpose.

```bash
docker run -it -d --name redis-standalone -p 6379:6379 redis redis-server --appendonly yes --requirepass examplepass
```

* Then, Install haredis:

```bash
pip install haredis
```

* Then, For Using FastAPI, you need install FastAPI dependencies.

```bash
pip install fastapi[all]
```

* Then create a file named `main.py` and copy the following code into it:

```python
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import asyncio
import time
from haredis import HaredisLockRelaseManager, set_up_logging
import redis
from redis import asyncio as aioredis

# Define Redis Connection Objects for both async and sync operations
_REDIS = redis.Redis(
    host="0.0.0.0",
    port=6379,
    db=0,
    password="examplepass",
    decode_responses=True,
    encoding="utf-8",
    max_connections=2**31
)
_AIOREDIS = aioredis.Redis(
    host="0.0.0.0",
    port=6379,
    db=0,
    password="examplepass",
    decode_responses=True,
    encoding="utf-8",
    max_connections=2**31
)

# Define Redis Lock Manager
RL_MANAGER = HaredisLockRelaseManager(
    _REDIS,
    _AIOREDIS,
    redis_logger=set_up_logging(console_log_level="DEBUG", logfile_log_level="DEBUG")
    )

# Create a connection pools
async def create_aioredis_pool() -> aioredis.ConnectionPool:
    return aioredis.ConnectionPool(
        host="0.0.0.0",
        port=6379,
        db=0,
        password="examplepass",
        decode_responses=True,
        encoding="utf-8",
        max_connections=2**31
    )
    
def create_redis_pool() -> redis.ConnectionPool:
    return redis.ConnectionPool(
        host="0.0.0.0",
        port=6379,
        db=0,
        password="examplepass",
        decode_responses=True,
        encoding="utf-8",
        max_connections=2**31
    )

# Get FastAPI Application
def get_application() -> FastAPI:
    _app = FastAPI()

    _app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    return _app
    
app = get_application()

# Configure startup - shutdown events
@app.on_event("startup")
async def startup_event():
        
    # Create Redis Connection Pools
    redis_pool = create_redis_pool()
    aioredis_pool = await create_aioredis_pool()
    
    # Create Redis Clients
    app.state.redis = redis.Redis(connection_pool=redis_pool)
    app.state.aioredis = aioredis.Redis(connection_pool=aioredis_pool)
        
    # Check Redis Connections
    ping = app.state.redis.ping()
    if not ping:
        raise Exception("Redis Connection Error!")
    print("Redis Connection OK!")
    
    ping = await app.state.aioredis.ping()
    if not ping:
        raise Exception("AioRedis Connection Error!")
    print("AioRedis Connection OK!")
                    

@app.on_event("shutdown")
async def shutdown_event():
    
    # Close Redis Clients    
    await app.state.aioredis.close()
    app.state.redis.close()


# Option 1: Define functions to be used in routes (if you want to use lock release decorator)
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
def sync_decorated_style(*args, **kwargs):
    
    param1 = kwargs.get('param1')
    param2 = kwargs.get('param2')
        
    time.sleep(10)
    
    result = param1 ** param2
    
    if result == 0:
        return {"multiply": {}}
        
    if param2 == 0:
        raise Exception("Param2 cannot be zero")
    
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
async def async_decorated_stsyle(*args, **kwargs):
    """Async Example Function without lock release."""
    
    param1 = kwargs.get('param1')
    param2 = kwargs.get('param2')
    
    print("Main Async function is running...")
    
    await asyncio.sleep(10)
    
    result = param1 ** param2
    
    if result == 0:
        return {"multiply": {}}
        
    if param2 == 0:
        raise Exception("Param2 cannot be zero")
    
    result = {"multiply": result}
            
    return result

# Option 2: Define functions to be used in routes (if you do not want to use lock release decorator)

## Functions without lock release, which will be used in lock release functions
def example_function(*args, **kwargs):
    
    param1 = kwargs.get('param1')
    param2 = kwargs.get('param2')
        
    time.sleep(10)
    
    result = param1 ** param2
    
    if result == 0:
        return {"multiply": {}}
        
    if param2 == 0:
        raise Exception("Param2 cannot be zero")
    
    result = {"multiply": result}
            
    return result

async def example_async_function(*args, **kwargs):
    
    param1 = kwargs.get('param1')
    param2 = kwargs.get('param2')
    
    print("Main Async function is running...")
    
    await asyncio.sleep(10)
    
    result = param1 ** param2
    
    if result == 0:
        return {"multiply": {}}
        
    if param2 == 0:
        raise Exception("Param2 cannot be zero")
    
    result = {"multiply": result}
            
    return result

## Functions with lock release which they wrap functions without lock release on top
async def async_event_rl_native(*args, **kwargs):
    
    result = await RL_MANAGER.aio_lock_release_with_stream(
        func=example_async_function,
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
        args=(),
        **kwargs
    )
               
    return result

async def sync_event_rl_native(*args, **kwargs):
                                            
    result = await RL_MANAGER.aio_lock_release_with_stream(
        func=example_function,
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
        args=(),
        **kwargs
    )
            
    return result

# Define routes for decorated style
@app.get("/sync_decorated_style")
async def sync_decorated_style_route(param1: int, param2: int):
    """Sync Example Function with lock release."""
    
    result = await sync_decorated_style(param1=param1, param2=param2)
    
    return result

@app.get("/async_decorated_style")
async def async_decorated_style_route(param1: int, param2: int):
    """Async Example Function with lock release."""
    
    result = await async_decorated_stsyle(param1=param1, param2=param2)
    
    return result


# Define routes for non-decorated style
@app.get("/sync_non_decorated_style")
async def sync_non_decorated_style_route(param1: int, param2: int):
    """Sync Example Function without lock release."""
    
    result = await sync_event_rl_native(param1=param1, param2=param2)
    
    return result

@app.get("/async_non_decorated_style")
async def async_non_decorated_style_route(param1: int, param2: int):
    """Async Example Function without lock release."""
    
    result = await async_event_rl_native(param1=param1, param2=param2)
    
    return result

if __name__ == "__main__":
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
    )
```

* Run the application:

```bash
python main.py
```

* Test the application:

```bash
curl -X 'GET' \
  'http://0.0.0.0:8000/sync_decorated_style?param1=23&param2=23' \
  -H 'accept: application/json'
```

You can send multiple requests at the same time and see that the lock is working.

### More Examples

More examples can be found in the `examples` folder.

## Usage Parameters

Usage parameters for `aio_lock_release_with_stream` function:

```python
"""
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
redis_availability_strategy (str): Redis availabilty strategy. Defaults to "error". If "error", raise exception
    if redis is not available, if "continue", continue execution of function without redis if redis is not available.
response_cache (int, optional): If provided, cache response with provided time in seconds. Defaults to None.
extend_cache_time (bool, optional): If True, extend cache time with response cache parameter. Defaults to False.
args (tuple, optional): Arguments for function. Defaults to ().
**kwargs (dict, optional): Keyword arguments for function. Defaults to {}.
"""
```

For `aio_lock_release_decorator_streams`, you don't need specify `func`, `args` and `**kwargs` parameter. It will be automatically set.