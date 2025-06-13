import requests
from typing import Optional, Dict
from pyetm.config.settings import settings
from pyetm.services.service_result import AuthenticationError, GenericError

class RequestsSession(requests.Session):
    """
    A requests.Session that:
      - Prefixes every path with settings.base_url
      - Adds Authorization header from settings.etm_api_token
      - Converts HTTP errors into GenericError subclasses
      #TODO: Expand/correct this docstring
    """

    def __init__(self, base_url: Optional[str] = None, token: Optional[str] = None):
        super().__init__()
        self.base_url = base_url or settings.base_url
        self.token = token or settings.etm_api_token

        # global headers
        self.headers.update({
            "Authorization": f"Token {self.token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        })

    def request(self, method: str, url: str, **kwargs) -> requests.Response:
        # Ensure we only pass a path to `url`; prefix with base_url
        full_url = url if url.startswith("http") else f"{self.base_url.rstrip('/')}/{url.lstrip('/')}"
        resp = super().request(method, full_url, **kwargs)
        self._handle_errors(resp)
        return resp

    # TODO: Implement more verbose error handling
    def _handle_errors(self, response: requests.Response):
        if response.status_code == 401:
            raise AuthenticationError("Invalid or missing ETM_API_TOKEN")
        if 400 <= response.status_code < 600:
            raise GenericError(f"HTTP {response.status_code}: {response.text}")
        # 2xx → OK
