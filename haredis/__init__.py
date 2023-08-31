"""
haredis: Redis/AioRedis extension module for Python
==================================

haredis is a Python module integrating redis/aioredis
lock-release algorithms with simple way.

It aims to provide simple and efficient solutions to lock-release problems
with streaming API.

"""

import logging

logger = logging.getLogger(__name__)

__version__ = "0.0.1"


__all__ = [
    "client",
    "core",
    "object",
]