"""Async HTTP session management for ETM API."""

from __future__ import annotations

import asyncio
import ssl
import threading
from pathlib import Path
from typing import Optional, Dict, Any, cast
from pydantic import BaseModel

import aiohttp
from pyetm.config.settings import get_settings


class ETMResponse(BaseModel):
    """
    Response object that works with both sync and async operations.
    """

    headers: Dict[str, str]
    url: str
    text: str = ""
    status_code: int
    _content: bytes = b""
    _json_data: Optional[Dict[str, Any]] = None

    @property
    def ok(self) -> bool:
        return 200 <= self.status_code < 300

    def json(self) -> Dict[str, Any]:  # type: ignore[override]
        """Parse response body as JSON."""
        if self._json_data is not None:
            return self._json_data

        import json

        return cast(Dict[str, Any], json.loads(self.text))

    # TODO: why encode again? I understand that curves are creating IO streams
    # but why so generic? Also, curves seem to be the only ones accessing this prop
    # We are making object unneccessarily heavy by keeping both text and content
    # and passing streams.
    # IDEA: handle conversion into stream in possible handling classes
    @property
    def content(self) -> bytes:
        """Get response content as bytes."""
        return self._content or self.text.encode("utf-8")

    def _format_error_message(self) -> str:
        """Format error message from API response in a readable way."""
        import json

        try:
            # Try to parse as JSON
            data = json.loads(self.text)

            # Handle {"errors": {...}} format
            if isinstance(data, dict) and "errors" in data:
                errors = data["errors"]

                # Handle {"errors": {"field": ["message1", "message2"]}}
                if isinstance(errors, dict):
                    messages = []
                    for field, field_errors in errors.items():
                        if isinstance(field_errors, list):
                            for error in field_errors:
                                messages.append(f"{field}: {error}")
                        else:
                            messages.append(f"{field}: {field_errors}")

                    if messages:
                        return f"{self.status_code}: {', '.join(messages)}"

                # Handle {"errors": ["message1", "message2"]}
                elif isinstance(errors, list):
                    return f"{self.status_code}: {', '.join(str(e) for e in errors)}"

            # Handle {"error": "message"} format
            if isinstance(data, dict) and "error" in data:
                return f"{self.status_code}: {data['error']}"

        except (json.JSONDecodeError, KeyError, TypeError):
            pass

        # Fall back to raw text (strip to avoid unnecessary whitespace)
        text = self.text.strip()
        return f"{self.status_code}: {text}" if text else f"{self.status_code}"

    def raise_for_status(self) -> None:
        """Raise appropriate exception for HTTP errors."""
        if self.status_code == 401:
            raise PermissionError("Invalid or missing ETM_API_TOKEN")

        if 400 <= self.status_code < 500:
            raise ValueError(self._format_error_message())
        if 500 <= self.status_code < 600:
            raise ConnectionError(self._format_error_message())


# TODO: Extract utils and organise private methods
# aiohttp usage looks legit - researching possibility to yield from pool [not likely]
class ETMSession:
    """Modern async session for ETM API interactions."""

    def __init__(
        self,
        base_url: Optional[str] = None,
        token: Optional[str] = None,
        ssl_verify: Optional[bool] = None,
        trust_env: Optional[bool] = None,
        ssl_cert_path: Optional[Path] = None,
    ):
        self.base_url = str(base_url or get_settings().base_url).rstrip("/")
        self.token = token or get_settings().etm_api_token
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/json",
        }

        # SSL configuration
        settings = get_settings()
        self.ssl_verify = ssl_verify if ssl_verify is not None else settings.ssl_verify
        self.trust_env = trust_env if trust_env is not None else settings.trust_env
        self.ssl_cert_path = ssl_cert_path or settings.ssl_cert_path

        self._session: Optional[aiohttp.ClientSession] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._loop_thread: Optional[threading.Thread] = None
        self._loop_started = threading.Event()

        self._start_loop_thread()

    def _start_loop_thread(self) -> None:
        """Start background thread with event loop."""

        def run_loop() -> None:
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
            self._loop_started.set()
            self._loop.run_forever()

        self._loop_thread = threading.Thread(target=run_loop, daemon=True)
        self._loop_thread.start()
        if not self._loop_started.wait(timeout=5.0):
            raise RuntimeError("Event loop thread failed to start within 5 seconds")

    def _ensure_session(self) -> None:
        """Ensure aiohttp session exists."""
        if self._session is None:
            if self._loop is None:
                raise RuntimeError("Event loop not initialized")
            future = asyncio.run_coroutine_threadsafe(self._create_session(), self._loop)
            future.result()

    async def _create_session(self) -> None:
        """Create aiohttp session with SSL configuration."""
        connector_kwargs: dict[str, Any] = {
            "limit": 10,
            "limit_per_host": 2,
            "keepalive_timeout": 120,
        }

        # Configure SSL verification
        ssl_context: Optional[ssl.SSLContext] | bool = None
        if not self.ssl_verify:
            # Disable SSL verification (for testing with self-signed certs)
            ssl_context = False
        elif self.ssl_cert_path:
            # Use custom CA certificate bundle
            ssl_context = ssl.create_default_context(cafile=str(self.ssl_cert_path))
        # else: ssl_context remains None, which uses Python's default SSL verification

        connector_kwargs["ssl"] = ssl_context
        connector = aiohttp.TCPConnector(**connector_kwargs)
        timeout = aiohttp.ClientTimeout(total=120, connect=120)

        self._session = aiohttp.ClientSession(
            headers=self.headers,
            connector=connector,
            timeout=timeout,
            trust_env=self.trust_env,
        )

    def request(self, method: str, url: str, **kwargs: Any) -> ETMResponse:
        """Make HTTP request (sync)."""
        self._ensure_session()
        if self._loop is None:
            raise RuntimeError("Event loop not initialized")
        future = asyncio.run_coroutine_threadsafe(
            self.async_request(method, url, **kwargs), self._loop
        )
        return future.result()

    def __getattr__(self, name: str) -> Any:
        """Method generation for HTTP verbs."""
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

        raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")

    async def async_request(self, method: str, url: str, **kwargs: Any) -> ETMResponse:
        """Make async HTTP request."""
        request_kwargs = self._build_request_kwargs(**kwargs)
        full_url = url if url.startswith("http") else f"{self.base_url}/{url.lstrip('/')}"

        if self._session is None:
            raise RuntimeError("Session not initialized")

        async with self._session.request(method, full_url, **request_kwargs) as response:
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

            # Check for HTTP errors after response is fully constructed
            etm_response.raise_for_status()

            return etm_response

    def _build_request_kwargs(self, **kwargs: Any) -> Dict[str, Any]:
        """Build request kwargs for aiohttp."""
        request_kwargs: Dict[str, Any] = {}

        if "files" in kwargs:
            form_data = aiohttp.FormData()

            for field_name, file_tuple in kwargs["files"].items():
                filename, file_content, content_type = file_tuple
                content = file_content.read() if hasattr(file_content, "read") else file_content
                form_data.add_field(
                    field_name, content, filename=filename, content_type=content_type
                )

            request_kwargs["data"] = form_data
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

        if "headers" in kwargs:
            headers: Dict[str, str] = dict(self.headers)
            headers.update(kwargs["headers"])
            request_kwargs["headers"] = headers

        return request_kwargs

    def close(self) -> None:
        """Close session and clean up resources."""
        if self._session and self._loop:
            future = asyncio.run_coroutine_threadsafe(self._session.close(), self._loop)
            future.result()

        if self._loop:
            self._loop.call_soon_threadsafe(self._loop.stop)
