import httpx
from typing import Optional
from pyetm.config.settings import get_settings


class AsyncRequestsSession:
    """
    An async HTTP client using httpx.AsyncClient that:
      - Prefixes every path with get_settings().base_url
      - Adds Authorization header from get_settings().etm_api_token
      - Converts HTTP errors into standard Python exceptions
    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        token: Optional[str] = None,
        timeout: float = 10.0,
    ):
        self.base_url = str(base_url or get_settings().base_url)
        self.token = token or get_settings().etm_api_token
        self.timeout = timeout
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            headers={
                "Authorization": f"Bearer {self.token}",
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
            timeout=self.timeout,
        )

    async def request(self, method: str, url: str, **kwargs) -> httpx.Response:
        full_url = (
            str(url)
            if url.startswith("http")
            else f"{str(self.base_url).rstrip('/')}/{url.lstrip('/')}"
        )
        resp = await self._client.request(method, full_url, **kwargs)
        self._handle_errors(resp)
        return resp

    def _handle_errors(self, resp: httpx.Response) -> None:
        if resp.status_code == 401:
            raise PermissionError("Invalid or missing ETM_API_TOKEN")
        if 400 <= resp.status_code < 500:
            raise ValueError(f"HTTP {resp.status_code}: {resp.text}")
        if 500 <= resp.status_code < 600:
            raise ConnectionError(f"HTTP {resp.status_code}: {resp.text}")

    async def close(self):
        await self._client.aclose()
