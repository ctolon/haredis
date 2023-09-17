from models.event import EventModel
from utils.common import Singleton

class EventController(metaclass=Singleton):
    __name__ = 'event'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.model = EventModel()
        
    # ------------------------ #
    # Business Logic Functions #
    # ------------------------ #
    
    def sync_event_without_rl(self, *args, **kwargs):
        result = self.model.sync_event_without_rl(self, *args, **kwargs)
        return result
    
    async def async_event_without_rl(self, *args, **kwargs):
        result = await self.model.sync_event_without_rl(self, *args, **kwargs)
        return result
    
    # ------------------------------------------------ #
    # Business Logic + RL Decorated Combined Functions #
    # ------------------------------------------------ #
    
    async def async_event_decorated(self, *args, **kwargs):
        result = await self.model.async_event_decorated(self, *args, **kwargs)
        return result
    
    async def sync_event_decorated(self, *args, **kwargs):
        result = await self.model.sync_event_decorated(self, *args, **kwargs)
        return result 
    
    # -------------------------------------------------------- #
    # Business Logic which wrapped from RL Decorator Functions #
    # -------------------------------------------------------- #
            
    async def async_event_decorated_wrapped(self, *args, **kwargs):
        result = await self.model.async_event_decorated_wrapped(self, *args, **kwargs)
        return result
    
    async def sync_event_decorated_wrapped(self, *args, **kwargs):
        result = await self.model.sync_event_decorated_wrapped(self, *args, **kwargs)
        return result 
    
    # ---------------------------------------------- #
    # Business Logic + RL Factory Combined Functions #
    # ---------------------------------------------- #
    
    async def async_event_rl_native(self, *args, **kwargs):
        result = await self.model.async_event_rl_native(self, *args, **kwargs)
        return result 
    
    async def sync_event_rl_native(self, *args, **kwargs):
        result = await self.model.sync_event_rl_native(self, *args, **kwargs)
        return result 
    
