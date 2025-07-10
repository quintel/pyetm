import requests
from typing import Optional, Dict
from pyetm.config.settings import get_settings


class RequestsSession(requests.Session):
    """
    A requests.Session that:
      - Prefixes every path with get_settings().base_url
      - Adds Authorization header from get_settings().etm_api_token
      - Converts HTTP errors into GenericError subclasses
    """

    def __init__(self, base_url: Optional[str] = None, token: Optional[str] = None):
        super().__init__()
        self.base_url = base_url or get_settings().base_url
        self.token = token or get_settings().etm_api_token

        # global headers
        self.headers.update(
            {
                "Authorization": f"Bearer {self.token}",
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
        )

    def request(self, method: str, url: str, **kwargs) -> requests.Response:
        # Ensure we only pass a path to `url`; prefix with base_url
        full_url = (
            str(url)
            if url.startswith("http")
            else f"{str(self.base_url).rstrip("/")}/{url.lstrip('/')}"
        )
        resp = super().request(method, full_url, **kwargs)
        self._handle_errors(resp)
        return resp

    def _handle_errors(self, resp: requests.Response) -> None:
        """
        Handle HTTP errors using standard exception types.
        """
        if resp.status_code == 401:
            raise PermissionError("Invalid or missing ETM_API_TOKEN")

        if 400 <= resp.status_code < 500:
            # Client errors (4xx)
            raise ValueError(f"HTTP {resp.status_code}: {resp.text}")

        if 500 <= resp.status_code < 600:
            # Server errors (5xx)
            raise ConnectionError(f"HTTP {resp.status_code}: {resp.text}")
