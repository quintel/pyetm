"""Base HTTP client for ETM API communication."""

from __future__ import annotations

import asyncio
import functools
from typing import Optional, Any, List, Dict

from pyetm.services.service_result import ServiceResult
from .session import ETMSession
from pyetm.config.settings import get_settings

MAX_CONCURRENT = 2


class BaseClient:
    """
    HTTP client with async capabilities for ETM API communication.

    Each instance is independent and can be configured with different
    tokens and base URLs, allowing multiple clients in the same script.
    """

    def __init__(self, token: Optional[str] = None, base_url: Optional[str] = None):
        """
        Initialize the BaseClient with authentication and connection details.

        Args:
            token (Optional[str]): API authentication token. If None, uses the token
                from application settings.
            base_url (Optional[str]): Base URL for API requests. If None, uses the
                base URL from application settings.
        """
        settings = get_settings()
        # Convert HttpUrl to string if needed
        base_url_str = base_url or (
            str(settings.base_url) if settings.base_url else None
        )
        self.session = ETMSession(
            base_url=base_url_str,
            token=token or settings.etm_api_token,
            ssl_verify=settings.ssl_verify,
            trust_env=settings.trust_env,
            ssl_cert_path=settings.ssl_cert_path,
        )

    def close(self) -> None:
        """
        Clean up resources and close the session.

        This method should be called when the client is no longer needed to properly
        release network connections and other resources.
        """
        self.session.close()


@functools.lru_cache(maxsize=1)
def get_client() -> BaseClient:
    """
    Get the default BaseClient instance.

    This function returns a cached client instance configured with settings
    from environment variables or .env file. For simple use cases, this provides
    a convenient default client without needing to explicitly create one.

    For advanced use cases requiring multiple clients with different configurations,
    create BaseClient instances directly.

    Returns:
        BaseClient: Cached default client instance

    Example:
        >>> from pyetm.clients import get_client
        >>> client = get_client()  # Uses env vars
        >>> response = client.session.get("/scenarios/123")
    """
    return BaseClient()


class AsyncBatchRunner:
    """
    Utility class for executing multiple HTTP requests concurrently.

    This class provides both asynchronous and synchronous methods for batch
    processing of HTTP requests with proper error handling and result wrapping.
    """

    # NOTE: are we stuck to the gather?
    # Can't we yield what is done somehow?
    @staticmethod
    async def batch_requests(
        session: ETMSession, requests: List[Dict[str, Any]], max_concurrent: int = 10
    ) -> List[ServiceResult[Any]]:
        """
        Execute multiple requests concurrently using asyncio.

        This method processes all requests in parallel and returns results in the
        same order as the input requests. Each result is wrapped in a ServiceResult
        for consistent error handling. Results can be either dict (for JSON responses)
        or ETMResponse objects (for non-JSON responses like CSV).
        """
        # Controls how many requests are in flight at the same time at the application level.
        semaphore = asyncio.Semaphore(max_concurrent)

        async def make_single_request(
            req: Dict[str, Any],
        ) -> ServiceResult[Any]:
            """
            Execute a single request and wrap the result in ServiceResult.

            Args:
                req (dict): Request specification containing method, url, and kwargs.

            Returns:
                ServiceResult: Wrapped result with either success data (dict for JSON,
                ETMResponse for non-JSON like CSV) or error details.
            """
            async with semaphore:
                try:
                    response = await session.async_request(
                        req["method"], req["url"], **req.get("kwargs", {})
                    )

                    # Success - wrap response data in ServiceResult
                    if response.ok:
                        data: Any
                        try:
                            data = response.json()
                        except Exception:
                            # For non-JSON responses (like CSV), return the response object
                            data = response

                        return ServiceResult.ok(data=data)
                    else:
                        return ServiceResult.fail(
                            errors=[f"HTTP {response.status_code}: {response.text}"]
                        )

                except PermissionError as e:
                    return ServiceResult.fail(errors=[f"Authentication error: {str(e)}"])
                except ValueError as e:
                    return ServiceResult.fail(errors=[f"Client error: {str(e)}"])
                except ConnectionError as e:
                    return ServiceResult.fail(errors=[f"Server error: {str(e)}"])
                except Exception as e:
                    return ServiceResult.fail(errors=[f"Unexpected error: {str(e)}"])

        # Execute all requests concurrently
        # TODO: yes, but they are still in a list, which is a predetermined structure
        # can we fire and yield?
        tasks = [make_single_request(req) for req in requests]
        return await asyncio.gather(*tasks)

    @staticmethod
    def batch_requests_sync(
        session: ETMSession, requests: List[Dict[str, Any]], max_concurrent: int = 10
    ) -> List[ServiceResult[Optional[Dict[str, Any]]]]:
        """
        Synchronous wrapper for batch_requests method.

        This method provides a synchronous interface to the async batch_requests
        functionality by running it in the session's event loop using
        asyncio.run_coroutine_threadsafe.

        Args:
            session (ETMSession): Active ETM session instance for making requests.
            requests (List[dict]): List of request specifications with the same
                format as batch_requests method.

        Returns:
            List[ServiceResult]: List of ServiceResult objects containing either
                success data or error information for each request.

        Note:
            This method blocks until all requests are completed and should only
            be used when async/await syntax is not available in the calling context.
        """
        coro = AsyncBatchRunner.batch_requests(session, requests, max_concurrent)
        if session._loop is None:
            raise RuntimeError("Event loop not initialized in session")
        future = asyncio.run_coroutine_threadsafe(coro, session._loop)
        return future.result()


# TODO: why is he just here?
# Helper function for runners that need batch operations
def make_batch_requests(
    client: BaseClient, requests: List[Dict[str, Any]]
) -> List[ServiceResult[Optional[Dict[str, Any]]]]:
    """
    Convenience function for making batch requests using a BaseClient instance.

    This helper function extracts the session from a BaseClient and delegates
    to AsyncBatchRunner.batch_requests_sync for execution.
    """
    return AsyncBatchRunner.batch_requests_sync(
        client.session, requests, MAX_CONCURRENT
    )
