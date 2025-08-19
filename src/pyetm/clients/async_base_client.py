from pyetm.utils.singleton import SingletonMeta
from .async_session import AsyncRequestsSession
from pyetm.config.settings import get_settings


class AsyncBaseClient(metaclass=SingletonMeta):
    """
    Singleton async HTTP client for interacting with the ETM API.
    Usage:
        client = AsyncBaseClient()
        response = await client.session.request(...)
    """

    def __init__(self, token: str = None, base_url: str = None, timeout: float = 10.0):
        self.session = AsyncRequestsSession(
            base_url=base_url or get_settings().base_url,
            token=token or get_settings().etm_api_token,
            timeout=timeout,
        )
