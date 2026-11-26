"""Base runner class for scenario operations."""

from typing import Any, Dict, List, Optional, TypeVar, Generic, cast
from abc import ABC
from ..service_result import ServiceResult
from pyetm.clients.base_client import AsyncBatchRunner, BaseClient, MAX_CONCURRENT

T = TypeVar("T")

PAGE_LIMIT = 100


def scenario_path(scenario: Any, suffix: str = "") -> str:
    """
    The path of a call on a Scenario's Session. MyETM relays everything under this base path to
    the Scenario's current Session, so no call names a Session itself. Without a suffix the path
    keeps its trailing slash: that addresses the Session, while no slash addresses the Scenario.
    """
    return f"/saved_scenarios/{scenario.id}/{suffix}"


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
        **kwargs: Any,
    ) -> ServiceResult[Any]:
        """
        Make an HTTP request. A JSON body comes back parsed, anything else as the response itself.
        Every failure, including an HTTP error, becomes a failed ServiceResult with its message.
        """
        try:
            resp = getattr(client.session, method.lower())(
                path, **cls._request_kwargs(method, payload, kwargs)
            )
        except Exception as e:
            return ServiceResult.fail([str(e)])

        try:
            return ServiceResult.ok(data=resp.json())
        except ValueError:
            return ServiceResult.ok(data=resp)

    @staticmethod
    def _request_kwargs(
        method: str, payload: Optional[Dict[str, Any]], kwargs: Dict[str, Any]
    ) -> Dict[str, Any]:
        """A payload is the JSON body of a write, or the query of a read."""
        request_kwargs = dict(kwargs)
        if payload is not None and "files" not in kwargs:
            body_key = "json" if method.upper() in ("POST", "PUT", "PATCH", "DELETE") else "params"
            request_kwargs[body_key] = payload
        return request_kwargs

    @classmethod
    def _make_myetm_request(
        cls, client: BaseClient, method: str, path: str, **kwargs: Any
    ) -> ServiceResult[Any]:
        """A request to one of MyETM's own resources, unwrapped from its {data, meta} envelope."""
        result = cls._make_request(client, method, path, **kwargs)
        if result.success and isinstance(result.data, dict) and "data" in result.data:
            return ServiceResult.ok(data=result.data["data"], errors=result.errors)
        return result

    @classmethod
    def _fetch_all_pages(cls, client: BaseClient, path: str) -> ServiceResult[List[Any]]:
        """Every item of a paginated MyETM index, following its pagination meta."""
        items: List[Any] = []
        page = 1
        while True:
            result = cls._make_request(client, "get", path, payload={"page": page, "limit": PAGE_LIMIT})
            if not result.success:
                return result
            body = cast(Dict[str, Any], result.data)
            items.extend(body["data"])
            if page >= body["meta"]["pagination"]["pages"]:
                return ServiceResult.ok(data=items)
            page += 1

    @classmethod
    def _make_batch_requests(
        cls, client: BaseClient, requests: List[Dict[str, Any]]
    ) -> List[ServiceResult[Any]]:
        """
        Make multiple requests concurrently.
        """
        formatted_requests = []
        for req in requests:
            kwargs = cls._request_kwargs(req["method"], req.get("payload"), req.get("kwargs") or {})
            formatted_requests.append({"method": req["method"], "url": req["path"], "kwargs": kwargs})

        return AsyncBatchRunner.batch_requests_sync(client.session, formatted_requests, MAX_CONCURRENT)

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
