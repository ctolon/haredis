__all__ = [
    'DEFAULT_RETRY_TIMES',
    'DEFAULT_RETRY_DELAY',
    'DEFAULT_TTL',
    'CLOCK_DRIFT_FACTOR',
    'RELEASE_LUA_SCRIPT'
    ]

DEFAULT_RETRY_TIMES = 3

DEFAULT_RETRY_DELAY = 200

DEFAULT_TTL = 100000

CLOCK_DRIFT_FACTOR = 0.01

# Reference:  http://redis.io/topics/distlock
# Section Correct implementation with a single instance

RELEASE_LUA_SCRIPT = """
    if redis.call("get",KEYS[1]) == ARGV[1] then
        return redis.call("del",KEYS[1])
    else
        return 0
    end
"""