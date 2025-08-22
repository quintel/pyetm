from __future__ import annotations

import asyncio
import threading
import sys
from typing import Optional, Dict
from dataclasses import dataclass

import aiohttp
from pyetm.config.settings import get_settings


@dataclass
class ETMResponse:
    """
    Response object that works with both sync and async operations.
    Compatible with requests.Response interface for backward compatibility.
    """

    status_code: int
    headers: Dict[str, str]
    url: str
    text: str = ""
    _content: bytes = b""
    _json_data: Optional[dict] = None

    @property
    def ok(self) -> bool:
        """Check if response was successful (2xx status)."""
        return 200 <= self.status_code < 300

    def json(self) -> dict:
        """Parse response as JSON."""
        if self._json_data is not None:
            return self._json_data

        import json

        return json.loads(self.text)

    @property
    def content(self) -> bytes:
        """Get response content as bytes."""
        return self._content or self.text.encode("utf-8")

    def raise_for_status(self) -> None:
        """Raise appropriate exception for HTTP errors."""
        if self.status_code == 401:
            raise PermissionError("Invalid or missing ETM_API_TOKEN")

        if 400 <= self.status_code < 500:
            raise ValueError(f"HTTP {self.status_code}: {self.text}")

        if 500 <= self.status_code < 600:
            raise ConnectionError(f"HTTP {self.status_code}: {self.text}")


class ETMSession:
    """Modern async session for ETM API interactions."""

    def __init__(self, base_url: Optional[str] = None, token: Optional[str] = None):
        self.base_url = str(base_url or get_settings().base_url).rstrip("/")
        self.token = token or get_settings().etm_api_token
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/json",
        }
        self.proxies = get_settings().proxy_servers or {}

        self._session: Optional[aiohttp.ClientSession] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._loop_thread: Optional[threading.Thread] = None
        self._loop_started = threading.Event()

        self._start_loop_thread()

    def _start_loop_thread(self):
        """Start background thread with event loop."""

        def run_loop():
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
            self._loop_started.set()
            self._loop.run_forever()

        self._loop_thread = threading.Thread(target=run_loop, daemon=True)
        self._loop_thread.start()
        self._loop_started.wait()

    def _ensure_session(self):
        """Ensure aiohttp session exists."""
        if self._session is None:
            future = asyncio.run_coroutine_threadsafe(
                self._create_session(), self._loop
            )
            future.result()

    async def _create_session(self):
        """Create aiohttp session."""
        connector_kwargs = {
            "limit": 100,
            "limit_per_host": 30,
            "keepalive_timeout": 30,
        }

        if sys.version_info < (3, 12, 9):
            connector_kwargs["enable_cleanup_closed"] = True

        connector = aiohttp.TCPConnector(**connector_kwargs)
        timeout = aiohttp.ClientTimeout(total=30, connect=10)

        self._session = aiohttp.ClientSession(
            headers=self.headers, connector=connector, timeout=timeout
        )

    def request(self, method: str, url: str, **kwargs) -> ETMResponse:
        """Make HTTP request (sync interface)."""
        self._ensure_session()
        future = asyncio.run_coroutine_threadsafe(
            self.async_request(method, url, **kwargs), self._loop
        )
        return future.result()

    def __getattr__(self, name: str):
        """Dynamic method generation for HTTP verbs."""
        if name in ["get", "post", "put", "patch", "delete"]:
            return lambda url, **kwargs: self.request(name.upper(), url, **kwargs)

        if name.startswith("async_") and name[6:] in [
            "get",
            "post",
            "put",
            "patch",
            "delete",
        ]:
            method = name[6:].upper()
            return lambda url, **kwargs: self.async_request(method, url, **kwargs)

        raise AttributeError(
            f"'{self.__class__.__name__}' object has no attribute '{name}'"
        )

    async def async_request(self, method: str, url: str, **kwargs) -> ETMResponse:
        """Make async HTTP request."""
        request_kwargs = self._build_request_kwargs(**kwargs)
        full_url = (
            url if url.startswith("http") else f"{self.base_url}/{url.lstrip('/')}"
        )

        async with self._session.request(
            method, full_url, **request_kwargs
        ) as response:
            etm_response = ETMResponse(
                status_code=response.status,
                headers=dict(response.headers),
                url=str(response.url),
            )

            content_type = response.headers.get("content-type", "").lower()

            if "application/json" in content_type:
                try:
                    etm_response._json_data = await response.json()
                    etm_response.text = await response.text()
                except Exception:
                    etm_response.text = await response.text()
            else:
                etm_response.text = await response.text()
                etm_response._content = await response.read()

            etm_response.raise_for_status()
            return etm_response

    def _build_request_kwargs(self, **kwargs) -> dict:
        """Build request kwargs for aiohttp."""
        request_kwargs = {}

        if "files" in kwargs:
            data = aiohttp.FormData()

            for field_name, file_tuple in kwargs["files"].items():
                filename, file_content, content_type = file_tuple
                content = (
                    file_content.read()
                    if hasattr(file_content, "read")
                    else file_content
                )
                data.add_field(
                    field_name, content, filename=filename, content_type=content_type
                )

            request_kwargs["data"] = data
            request_kwargs["headers"] = {
                k: v for k, v in self.headers.items() if k != "Content-Type"
            }
            return request_kwargs

        if "json" in kwargs:
            request_kwargs["json"] = kwargs["json"]
        if "params" in kwargs:
            request_kwargs["params"] = kwargs["params"]
        if "data" in kwargs:
            request_kwargs["data"] = kwargs["data"]

        if self.proxies:
            request_kwargs["proxy"] = self.proxies.get(
                "http", self.proxies.get("https")
            )

        if "headers" in kwargs:
            headers = dict(self.headers)
            headers.update(kwargs["headers"])
            request_kwargs["headers"] = headers

        return request_kwargs

    def close(self):
        """Close session and clean up resources."""
        if self._session:
            future = asyncio.run_coroutine_threadsafe(self._session.close(), self._loop)
            future.result()

        if self._loop:
            self._loop.call_soon_threadsafe(self._loop.stop)
