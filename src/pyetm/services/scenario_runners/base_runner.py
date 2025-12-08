from typing import Any, Dict, List, Optional, TypeVar, Generic, Protocol
from abc import ABC, abstractmethod
from ..service_result import ServiceResult
from pyetm.clients.base_client import BaseClient, make_batch_requests

T = TypeVar("T")


class ScenarioIdentifier(Protocol):
    """Protocol for objects with a scenario or saved_scenario ID."""

    @property
    def id(self) -> int: ...


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

    @classmethod
    def _validate_required_fields(
        cls, data: Dict[str, Any], required_keys: List[str]
    ) -> List[str]:
        """
        Check for missing required fields.
        """
        missing = [key for key in required_keys if key not in data]
        if missing:
            return [f"Missing required fields: {', '.join(missing)}"]
        return []

    @staticmethod
    def _filter_allowed_fields(
        data: Dict[str, Any],
        allowed_keys: List[str],
        context: str,
    ) -> tuple[Dict[str, Any], List[str]]:
        """
        Filter dictionary to only allowed keys, returning filtered data and warnings.

        Args:
            data: Input dictionary to filter
            allowed_keys: List of keys to keep
            context: Description for warning messages (e.g. "create saved scenario")

        Returns:
            (filtered_data, warnings) tuple
        """
        filtered = {k: v for k, v in data.items() if k in allowed_keys}
        ignored_keys = set(data.keys()) - set(filtered.keys())
        warnings = [
            f"Ignoring invalid field for {context}: {key!r}" for key in ignored_keys
        ]
        return filtered, warnings

    @classmethod
    def _validate_response_keys(
        cls, data: Dict[str, Any], required_keys: List[str], fill_missing: bool = False
    ) -> tuple[Dict[str, Any], List[str]]:
        """
        Validate that response contains required keys.

        Args:
            data: Response data to validate
            required_keys: Keys that should be present
            fill_missing: If True, add None for missing keys; if False, only warn

        Returns:
            (validated_data, warnings) tuple
        """
        warnings = []
        result = data.copy() if fill_missing else data

        for key in required_keys:
            if key not in data:
                warnings.append(f"Missing field in response: {key!r}")
                if fill_missing:
                    result[key] = None

        return result, warnings

    @staticmethod
    @abstractmethod
    def run(client: BaseClient, scenario: Any, **kwargs) -> ServiceResult[T]:
        """Subclasses must implement this method."""
        pass
