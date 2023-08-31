"""Example API."""

from fastapi import APIRouter, Request
from controllers.event import EventController
from models import ExampleRequest, AcquireLockRequest


router = APIRouter(
    tags=["fastapi-redis-api"],
    dependencies=[],
    responses={404: {"status": "error", "error": "Not found"}}
)


@router.post(
    "/example-sync-xread-cache",
    summary="add desc here",
    description="add desc here"
    )
async def example_sync_xread_cache(req: Request, example_request: ExampleRequest):
    
    param1 = example_request.param1
    param2 = example_request.param2
        
    # Initialize the lock controller
    event_controller = EventController()
    
    # Call Acquire the lock function from model
    result = await event_controller.sync_xread_cache_queue(
        req=req,
        param1=param1,
        param2=param2
    )
    
    return result

@router.post(
    "/example-async-xread-cache",
    summary="add desc here",
    description="add desc here"
    )
async def example_async_xread_cache(req: Request, example_request: ExampleRequest):
    
    param1 = example_request.param1
    param2 = example_request.param2
        
    # Initialize the lock controller
    event_controller = EventController()
    
    # Call Acquire the lock function from model
    result = await event_controller.async_xread_cache_queue(
        req=req,
        param1=param1,
        param2=param2
    )
    
    return result