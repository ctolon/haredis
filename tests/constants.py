class Endpoints:
    sync_event_without_rl = "sync-event-without-rl"
    async_event_without_rl = "async-event-without-rl"
    async_event_decorated = "async-event-decorated"
    sync_event_decorated = "sync-event-decorated"
    async_event_decorated_wrapped = "async-event-decorated-wrapped"
    sync_event_decorated_wrapped = "sync-event-decorated-wrapped"
    async_event_rl_native = "async-event-rl-native"
    sync_event_rl_native = "sync-event-rl-native"
    
class Datas:
    
    CLEAN_SAMPLE_1 = {
        "param1": 2,
        "param2": 4
    }
    
    CLEAN_SAMPLE_2 = {
        "param1": 3,
        "param2": 5
    }
    
    EXCEPTION_SAMPLE_1 = {
        "param1": 0,
        "param2": 0
    }
    
    EXCEPTION_SAMPLE_2 = {
        "param1": 3,
        "param2": 0
    }
    
class PORTS:
    
    API_PORT_1 = 5703
    API_PORT_2 = 5704
    API_PORT_3 = 5705
    
HOST = "0.0.0.0"