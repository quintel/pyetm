from __future__ import annotations

import asyncio
from typing import Optional, Any, List

from pyetm.utils.singleton import SingletonMeta
from pyetm.services.service_result import ServiceResult
from .session import ETMSession
from pyetm.config.settings import get_settings


class BaseClient(metaclass=SingletonMeta):
    """
    Singleton HTTP client with async capabilities.
    """

    def __init__(self, token: Optional[str] = None, base_url: Optional[str] = None):
        self.session = ETMSession(
            base_url=base_url or get_settings().base_url,
            token=token or get_settings().etm_api_token,
        )

    def close(self):
        """Clean up resources."""
        self.session.close()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


class AsyncBatchRunner:

    @staticmethod
    async def batch_requests(
        session: ETMSession, requests: List[dict]
    ) -> List[ServiceResult]:
        """
        Execute multiple requests concurrently.

        Args:
            session: ETMSession instance
            requests: List of request specifications with keys:
                - method: HTTP method
                - url: URL path
                - kwargs: Additional request parameters
        """

        async def make_single_request(req: dict) -> ServiceResult:
            """Execute a single request and wrap in ServiceResult."""
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
        tasks = [make_single_request(req) for req in requests]
        return await asyncio.gather(*tasks)

    @staticmethod
    def batch_requests_sync(
        session: ETMSession, requests: List[dict]
    ) -> List[ServiceResult]:
        """
        Sync wrapper for batch_requests.

        Args:
            session: ETMSession instance
            requests: List of request specifications

        Returns:
            List of ServiceResult objects
        """
        coro = AsyncBatchRunner.batch_requests(session, requests)
        future = asyncio.run_coroutine_threadsafe(coro, session._loop)
        return future.result()


# Helper function for runners that need batch operations
def make_batch_requests(
    client: BaseClient, requests: List[dict]
) -> List[ServiceResult]:
    return AsyncBatchRunner.batch_requests_sync(client.session, requests)
