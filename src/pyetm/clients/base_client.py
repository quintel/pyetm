from pyetm.utils.singleton import SingletonMeta
from .session import RequestsSession
from pyetm.config.settings import settings

class BaseClient(metaclass=SingletonMeta):
    """
    Singleton HTTP client for interacting with the ETM API.
    """
    def __init__(self, token: str = None, base_url: str = None):
        self.session = RequestsSession(
            base_url=base_url or settings.base_url,
            token=token or settings.etm_api_token
        )
