"""Extended Object Implementation for haredis"""

from ._haredlock import HAredlock, HAAioredlock

__all__ = [
    "HAredlock",
    "HAAioredlock"
    ]