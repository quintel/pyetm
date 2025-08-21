from typing import Any, Dict, List, Optional, TypeVar, Generic
from abc import ABC, abstractmethod
from ..service_result import ServiceResult
from pyetm.clients.base_client import BaseClient, make_batch_requests

T = TypeVar("T")


class BaseRunner(ABC, Generic[T]):
    """
    Base class for all API runners that handles common HTTP request patterns
    and error handling for both read and write operations.
    """

    @classmethod
    def _make_request(
        cls,
        client: BaseClient,
        method: str,
        path: str,
        payload: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> ServiceResult[Any]:
        """
        Make an HTTP request and handle common error patterns.

        Args:
            client: The HTTP client to use
            method: HTTP method (get, post, put, patch, delete)
            path: API endpoint path
            payload: Data to send in request body (for write operations)
            **kwargs: Additional arguments passed to the request

        Returns:
            ServiceResult.ok(data) on success (2xx responses)
            ServiceResult.fail(errors) on any error
        """
        try:
            # Prepare request arguments
            request_kwargs = dict(kwargs)

            # Handle payload based on HTTP method
            if payload is not None and "files" not in kwargs:
                if method.upper() in ["POST", "PUT", "PATCH"]:
                    request_kwargs["json"] = payload
                else:
                    # For GET/DELETE, treat payload as query parameters
                    request_kwargs["params"] = payload

            # Make the request
            resp = getattr(client.session, method.lower())(path, **request_kwargs)

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

    @classmethod
    def _make_batch_requests(
        cls, client: BaseClient, requests: List[Dict[str, Any]]
    ) -> List[ServiceResult[Any]]:
        """
        Make multiple requests concurrently.
        """
        formatted_requests = []
        for req in requests:
            formatted = {"method": req["method"], "url": req["path"], "kwargs": {}}

            if req.get("payload") and "files" not in req.get("kwargs", {}):
                if req["method"].upper() in ["POST", "PUT", "PATCH"]:
                    formatted["kwargs"]["json"] = req["payload"]
                else:
                    formatted["kwargs"]["params"] = req["payload"]

            # Merge any additional kwargs
            if req.get("kwargs"):
                formatted["kwargs"].update(req["kwargs"])

            formatted_requests.append(formatted)

        return make_batch_requests(client, formatted_requests)

    @staticmethod
    @abstractmethod
    def run(client: BaseClient, scenario: Any, **kwargs) -> ServiceResult[T]:
        """Subclasses must implement this method."""
        pass
