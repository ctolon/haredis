import pathlib
import asyncio

import uvicorn
from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware
from haredis.client import RedisClient, AioRedisClient

from routes.event import router as event_router
from config import RedisSettings, APISettings


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
        
    #loop = safe_set_event_loop()
    #print("Event loop started.")
    #app.state.loop = loop
        
    redis_client = RedisClient(
        #host=RedisSettings.HOST,
        host="0.0.0.0",
        port=RedisSettings.PORT,
        db=RedisSettings.DB,
        password=RedisSettings.PASSWORD,
        decode_responses=True,
        encoding="utf-8",
        max_connections=2**31
        )
    
    aioredis_client = AioRedisClient(
        host="0.0.0.0",
        port=RedisSettings.PORT,
        db=RedisSettings.DB,
        password=RedisSettings.PASSWORD,
        decode_responses=True,
        encoding="utf-8",
        max_connections=2**31
        )
    
    redis_client.connection_test()
    await aioredis_client.connection_test()
    
    app.state.aioredis = await aioredis_client.get_aioredis_client()  
    app.state.redis = redis_client.get_redis_client()
    

@app.on_event("shutdown")
async def shutdown_event():
    
    #if app.state.loop.is_running():
        #print("WARNING: Event loop is still running. It will be closed.")
        #app.state.loop.stop()
        # app.state.loop.close()
        # print("Event loop closed.")
    
    await app.state.aioredis.close_client()
    app.state.redis.close_client()


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