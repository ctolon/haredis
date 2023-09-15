import pathlib
import asyncio

import uvicorn
from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware
from haredis.client import HaredisClient, AioHaredisClient
import redis
from redis import asyncio as aioredis

from routes.event import router as event_router
from config import RedisSettings, APISettings

async def create_aioredis_pool() -> aioredis.ConnectionPool:
    return aioredis.ConnectionPool(
        #host=RedisSettings.HOST,
        host="0.0.0.0",
        port=RedisSettings.PORT,
        db=RedisSettings.DB,
        password=RedisSettings.PASSWORD,
        decode_responses=True,
        encoding="utf-8",
        max_connections=2**31
    )
    
def create_redis_pool() -> redis.ConnectionPool:
    return redis.ConnectionPool(
        # host=RedisSettings.HOST,
        host="0.0.0.0",
        port=RedisSettings.PORT,
        db=RedisSettings.DB,
        password=RedisSettings.PASSWORD,
        decode_responses=True,
        encoding="utf-8",
        max_connections=2**31
    )


def get_application() -> FastAPI:
    _app = FastAPI(
        title="[TUTORIAL] haredis - Redis Standalone",
        description="haredis with Redis Standalone setup example API",
        debug=False
    )

    _app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    return _app
    
app = get_application()

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
                    
    # Create Haredis Clients
    app.state.haredis = HaredisClient(client_conn=app.state.redis)
    app.state.aioharedis = AioHaredisClient(client_conn=app.state.aioredis)
    

@app.on_event("shutdown")
async def shutdown_event():
    
    # Close Redis Clients    
    await app.state.aioredis.close()
    app.state.redis.close()


app.include_router(event_router, prefix="/haredis")


if __name__ == '__main__':
    
    
    # For Development
    uvicorn.run(
        "api:app",
        host=APISettings.HOST,
        port=APISettings.PORT,
        log_level="info",
        reload=True
    )