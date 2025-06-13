from ..utils.singleton import SingletonMeta
from .session import RequestsSession
from ..config.config import config
from ..services.service_result import GenericError

class BaseClient(metaclass=SingletonMeta):
    """
    Singleton HTTP client for interacting with the ETM API.
    """
    def __init__(self, token: str = None, base_url: str = None):
        self.session = RequestsSession(
            base_url=base_url or config.base_url,
            token=token or config.etm_api_token
        )
