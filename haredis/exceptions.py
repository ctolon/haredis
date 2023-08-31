"""
This module includes all custom warnings and error classes used across haredis.
"""

__all__ = [
    "HAredlockError",
    "HALockAcquiringError",
    "HALockRuntimeError",
    "HALockError"
]

class HAredlockError(Exception):
    """
    Base exception for HAredlock
    """


class HALockError(HAredlockError):
    """
    Error in acquiring or releasing the lock
    """


class HALockAcquiringError(HALockError):
    """
    Error in acquiring the lock during normal operation
    """


class HALockRuntimeError(HALockError):
    """
    Error in acquiring or releasing the lock due to an unexpected event
    """