"""The HTTP transport to MyETM."""

from __future__ import annotations

import asyncio
import json
import ssl
import threading
from importlib.metadata import version
from pathlib import Path
from types import TracebackType
from typing import Any, cast

import aiohttp
from pydantic import BaseModel

from pyetm.config.settings import get_settings

USER_AGENT = f"pyetm/{version('pyetm')}"
TOKEN_HELP = "Get your token at https://my.energytransitionmodel.com/identity/tokens"


class ETMResponse(BaseModel):
    """Response object that works with both sync and async operations.
    """

    headers: dict[str, str]
    url: str
    text: str = ""
    status_code: int
    _content: bytes = b""
    _json_data: dict[str, Any] | None = None

    @property
    def ok(self) -> bool:
        return 200 <= self.status_code < 300

    def json(self) -> dict[str, Any]:  # type: ignore[override]
        """Parse response body as JSON."""
        if self._json_data is not None:
            return self._json_data

        return cast(dict[str, Any], json.loads(self.text))

    # TODO: why encode again? I understand that curves are creating IO streams
    # but why so generic? Also, curves seem to be the only ones accessing this prop
    # We are making object unneccessarily heavy by keeping both text and content
    # and passing streams.
    # IDEA: handle conversion into stream in possible handling classes
    @property
    def content(self) -> bytes:
        """Get response content as bytes."""
        return self._content or self.text.encode("utf-8")

    def error_message(self) -> str:
        """The status with every message the body carries, in MyETM's v4 grammar or ETEngine's own."""
        messages = _error_messages(self._body())
        if not messages and self.text.strip():
            messages = [self.text.strip()]
        return ": ".join([str(self.status_code), ", ".join(messages)]) if messages else str(self.status_code)

    def _body(self) -> Any:
        try:
            return self.json()
        except (json.JSONDecodeError, TypeError):
            return None

    def raise_for_status(self, token: str | None = None) -> None:
        """Raise PermissionError for 401 and 403, ValueError for other 4xx, ConnectionError for 5xx."""
        if self.status_code == 401:
            raise PermissionError(self._unauthenticated_message(token))
        if self.status_code == 403:
            raise PermissionError(self.error_message())
        if 400 <= self.status_code < 500:
            raise ValueError(self.error_message())
        if self.status_code >= 500:
            raise ConnectionError(self.error_message())

    def _unauthenticated_message(self, token: str | None) -> str:
        if token:
            return f"{self.error_message()}. Your ETM_API_TOKEN was refused: check it is correct and has not expired. {TOKEN_HELP}"
        return f"{self.error_message()}. This needs an ETM_API_TOKEN, and none is set. {TOKEN_HELP}"


def _error_messages(body: Any) -> list[str]:
    """Flatten a body's errors: v4 objects, ETEngine's {field: [messages]}, a list, or one string."""
    if not isinstance(body, dict):
        return []
    errors = body.get("errors", body.get("error"))
    if isinstance(errors, dict):
        return [f"{field}: {message}" for field, messages in errors.items() for message in _as_list(messages)]
    return [_v4_error(error) if isinstance(error, dict) else str(error) for error in _as_list(errors)]


def _v4_error(error: dict[str, Any]) -> str:
    message = ": ".join(str(part) for part in (error.get("code"), error.get("detail")) if part)
    pointer = (error.get("source") or {}).get("pointer")
    return f"{message} (at {pointer})" if pointer else message


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


# TODO: Extract utils and organise private methods
# aiohttp usage looks legit - researching possibility to yield from pool [not likely]
class ETMClient:
    """The HTTP transport to MyETM, sync and async, on a background event loop."""

    def __init__(
        self,
        base_url: str | None = None,
        token: str | None = None,
        ssl_verify: bool | None = None,
        trust_env: bool | None = None,
        ssl_cert_path: Path | None = None,
    ):
        self.base_url = str(base_url or get_settings().base_url).rstrip("/")
        self.token = token or get_settings().etm_api_token
        self.headers = {"Accept": "application/json", "User-Agent": USER_AGENT}
        if self.token:
            self.headers["Authorization"] = f"Bearer {self.token}"

        # SSL configuration
        settings = get_settings()
        self.ssl_verify = ssl_verify if ssl_verify is not None else settings.ssl_verify
        self.trust_env = trust_env if trust_env is not None else settings.trust_env
        self.ssl_cert_path = ssl_cert_path or settings.ssl_cert_path

        self._session: aiohttp.ClientSession | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._loop_thread: threading.Thread | None = None
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
        ssl_context: ssl.SSLContext | None | bool = None
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
            etm_response.raise_for_status(token=self.token)

            return etm_response

    def _build_request_kwargs(self, **kwargs: Any) -> dict[str, Any]:
        """Build request kwargs for aiohttp."""
        request_kwargs: dict[str, Any] = {}

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
            headers: dict[str, str] = dict(self.headers)
            headers.update(kwargs["headers"])
            request_kwargs["headers"] = headers

        return request_kwargs

    def close(self) -> None:
        """Close session and clean up resources."""
        if self._session and self._loop:
            future = asyncio.run_coroutine_threadsafe(self._session.close(), self._loop)
            future.result()
            self._session = None

        if self._loop:
            self._loop.call_soon_threadsafe(self._loop.stop)

        if self._loop_thread:
            self._loop_thread.join(timeout=5)
            self._loop_thread = None

        if self._loop and not self._loop.is_closed():
            self._loop.close()
            self._loop = None

    def __enter__(self) -> ETMClient:
        """Enter the runtime context, returning this session."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Exit the runtime context, closing the session."""
        self.close()
