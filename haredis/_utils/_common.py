from typing import Any
import json

class _Singleton (type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(_Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

def _safe_json(data): 
    if data is None: 
        return True 
    elif isinstance(data, (bool, int, float)): 
        return True 
    elif isinstance(data, (tuple, list)): 
        return all(_safe_json(x) for x in data) 
    elif isinstance(data, dict): 
        return all(isinstance(k, str) and _safe_json(v) for k, v in data.items()) 
    return False 

def _try_json_serialize(x: Any):
    try:
        x = json.dumps(x) if isinstance(dict, x) else x
        return x
    except TypeError as e:
        return x
    except OverflowError as e:
        raise OverflowError(e)
    
def _try_json_deserialize(x: Any):
    try:
        x = json.loads(x) if isinstance(str, x) else x
        return x
    except TypeError as e:
        return x
    except OverflowError as e:
        raise OverflowError(e)