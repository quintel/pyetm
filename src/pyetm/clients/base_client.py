from __future__ import annotations

import asyncio
from typing import Optional, Any, List

from pyetm.utils.singleton import SingletonMeta
from pyetm.services.service_result import ServiceResult
from .session import ETMSession
from pyetm.config.settings import get_settings


# TODO: like this it feels unnecessary
class BaseClient(metaclass=SingletonMeta):
    """
    Singleton HTTP client with async capabilities.
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
        self.session = ETMSession(
            base_url=base_url or get_settings().base_url,
            token=token or get_settings().etm_api_token,
        )

    def close(self):
        """
        Clean up resources and close the session.

        This method should be called when the client is no longer needed to properly
        release network connections and other resources.
        """
        self.session.close()


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
        session: ETMSession, requests: List[dict]
    ) -> List[ServiceResult]:
        """
        Execute multiple requests concurrently using asyncio.

        This method processes all requests in parallel and returns results in the
        same order as the input requests. Each result is wrapped in a ServiceResult
        for consistent error handling.
        """

        async def make_single_request(req: dict) -> ServiceResult:
            """
            Execute a single request and wrap the result in ServiceResult.

            Args:
                req (dict): Request specification containing method, url, and kwargs.

            Returns:
                ServiceResult: Wrapped result with either success data or error details.
            """
            try:
                response = await session.async_request(
                    req["method"], req["url"], **req.get("kwargs", {})
                )

                # Success - wrap response data in ServiceResult
                if response.ok:
                    try:
                        data = response.json()
                    except Exception:
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
        session: ETMSession, requests: List[dict]
    ) -> List[ServiceResult]:
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
        coro = AsyncBatchRunner.batch_requests(session, requests)
        future = asyncio.run_coroutine_threadsafe(coro, session._loop)
        return future.result()


# TODO: why is he just here?
# Helper function for runners that need batch operations
def make_batch_requests(
    client: BaseClient, requests: List[dict]
) -> List[ServiceResult]:
    """
    Convenience function for making batch requests using a BaseClient instance.

    This helper function extracts the session from a BaseClient and delegates
    to AsyncBatchRunner.batch_requests_sync for execution.
    """
    return AsyncBatchRunner.batch_requests_sync(client.session, requests)
