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
    "/sync-event-without-rl",
    summary="add desc here",
    description="add desc here"
    )
def sync_event_without_rl(req: Request, example_request: ExampleRequest):
    
    param1 = example_request.param1
    param2 = example_request.param2
        
    # Initialize the lock controller
    event_controller = EventController()
    
    # Call Acquire the lock function from model
    result = event_controller.sync_event_without_rl(
        req=req,
        param1=param1,
        param2=param2
    )
    
    return result

@router.post(
    "/async-event-without-rl",
    summary="add desc here",
    description="add desc here"
    )
async def async_event_without_rl(req: Request, example_request: ExampleRequest):
    
    param1 = example_request.param1
    param2 = example_request.param2
        
    # Initialize the lock controller
    event_controller = EventController()
    
    # Call Acquire the lock function from model
    result = await event_controller.async_event_without_rl(
        req=req,
        param1=param1,
        param2=param2
    )
    
    return result

@router.post(
    "/async-event-decorated",
    summary="add desc here",
    description="add desc here"
    )
async def async_event_decorated(req: Request, example_request: ExampleRequest):
    
    param1 = example_request.param1
    param2 = example_request.param2
        
    # Initialize the lock controller
    event_controller = EventController()
    
    # Call Acquire the lock function from model
    result = await event_controller.async_event_decorated(
        req=req,
        param1=param1,
        param2=param2
    )
    
    return result

@router.post(
    "/sync-event-decorated",
    summary="add desc here",
    description="add desc here"
    )
async def sync_event_decorated(req: Request, example_request: ExampleRequest):
    
    param1 = example_request.param1
    param2 = example_request.param2
        
    # Initialize the lock controller
    event_controller = EventController()
    
    # Call Acquire the lock function from model
    result = await event_controller.sync_event_decorated(
        req=req,
        param1=param1,
        param2=param2
    )
    
    return result

@router.post(
    "/async-event-decorated-wrapped",
    summary="add desc here",
    description="add desc here"
    )
async def async_event_decorated_wrapped(req: Request, example_request: ExampleRequest):
    
    param1 = example_request.param1
    param2 = example_request.param2
        
    # Initialize the lock controller
    event_controller = EventController()
    
    # Call Acquire the lock function from model
    result = await event_controller.async_event_decorated_wrapped(
        req=req,
        param1=param1,
        param2=param2
    )
    
    return result

@router.post(
    "/sync-event-decorated-wrapped",
    summary="add desc here",
    description="add desc here"
    )
async def sync_event_decorated_wrapped(req: Request, example_request: ExampleRequest):
    
    param1 = example_request.param1
    param2 = example_request.param2
        
    # Initialize the lock controller
    event_controller = EventController()
    
    # Call Acquire the lock function from model
    result = await event_controller.sync_event_decorated_wrapped(
        req=req,
        param1=param1,
        param2=param2
    )
    
    return result

@router.post(
    "/async-event-rl-native",
    summary="add desc here",
    description="add desc here"
    )
async def async_event_rl_native(req: Request, example_request: ExampleRequest):
    
    param1 = example_request.param1
    param2 = example_request.param2
        
    # Initialize the lock controller
    event_controller = EventController()
    
    # Call Acquire the lock function from model
    result = await event_controller.async_event_rl_native(
        req=req,
        param1=param1,
        param2=param2
    )
    
    return result

@router.post(
    "/sync-event-rl-native",
    summary="add desc here",
    description="add desc here"
    )
async def sync_event_rl_native(req: Request, example_request: ExampleRequest):
    
    param1 = example_request.param1
    param2 = example_request.param2
        
    # Initialize the lock controller
    event_controller = EventController()
    
    # Call Acquire the lock function from model
    result = await event_controller.sync_event_rl_native(
        req=req,
        param1=param1,
        param2=param2
    )
    
    return result