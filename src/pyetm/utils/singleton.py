"""Singleton metaclass implementation."""

import threading
from typing import Any, Dict, Type


class SingletonMeta(type):
    """
    A thread-safe singleton metaclass implementation.

    Attributes:
        _instances (dict): Class-level dictionary storing singleton instances
        _lock (threading.Lock): Thread lock to ensure thread-safe instance creation
    """

    _instances: Dict[Type[Any], Any] = {}
    _lock = threading.Lock()

    def __call__(cls, *args: Any, **kwargs: Any) -> Any:
        with cls._lock:
            if cls not in cls._instances:
                cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]
