from typing import Any, Dict, Optional, TypeVar, Generic
from abc import ABC, abstractmethod
from ..service_result import ServiceResult
from pyetm.clients.base_client import BaseClient

T = TypeVar("T")


class BaseRunner(ABC, Generic[T]):
    """
    Base class for all API runners that handles common HTTP request patterns
    and error handling.
    """

    @classmethod
    def _make_request(
        cls, client: BaseClient, method: str, path: str, **kwargs
    ) -> ServiceResult[Any]:
        """
        Make an HTTP request and handle common error patterns.

        Returns:
            ServiceResult.ok(data) on success (2xx responses)
            ServiceResult.fail(errors) on any error
        """
        try:
            resp = getattr(client.session, method.lower())(path, **kwargs)

            if resp.ok:
                # For JSON responses, parse automatically
                try:
                    return ServiceResult.ok(data=resp.json())
                except ValueError:
                    # Not JSON, return raw response
                    return ServiceResult.ok(data=resp)

            # HTTP-level failure is breaking
            return ServiceResult.fail([f"{resp.status_code}: {resp.text}"])

        except (PermissionError, ValueError, ConnectionError) as e:
            # These are HTTP errors from our _handle_errors method
            return ServiceResult.fail([str(e)])
        except Exception as e:
            # Any other unexpected exception is treated as breaking
            return ServiceResult.fail([str(e)])

    @staticmethod
    @abstractmethod
    def run(client: BaseClient, scenario: Any, **kwargs) -> ServiceResult[T]:
        """Subclasses must implement this method."""
        pass
