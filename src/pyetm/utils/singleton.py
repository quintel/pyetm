import threading


class SingletonMeta(type):
    """
    A thread-safe singleton metaclass implementation.

    Attributes:
        _instances (dict): Class-level dictionary storing singleton instances
        _lock (threading.Lock): Thread lock to ensure thread-safe instance creation
    """

    _instances = {}
    _lock = threading.Lock()

    def __call__(cls, *args, **kwargs):
        with cls._lock:
            if cls not in cls._instances:
                cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]
