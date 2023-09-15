from models.event import EventModel
from utils.common import Singleton

class EventController(metaclass=Singleton):
    __name__ = 'event'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.model = EventModel()
            
    async def sync_xread_cache_queue(self, *args, **kwargs):
        result =  await self.model.sync_xread_cache_queue(self, *args, **kwargs)
        return result
    
    async def async_xread_cache_queue(self, *args, **kwargs):
        result = await self.model.async_xread_cache_queue(self, *args, **kwargs)
        return result
    
    async def async_event_decorated(self, *args, **kwargs):
        result = await self.model.async_event_decorated(self, *args, **kwargs)
        return result
    
    def sync_event_decorated(self, *args, **kwargs):
        result = self.model.sync_event_decorated(self, *args, **kwargs)
        return result

    async def sync_event_decorated_2(self, *args, **kwargs):
        result = await self.model.sync_event_decorated(self, *args, **kwargs)
        return result 