"""Service for fetching user collections (MyETM)."""

from typing import Any, Dict, List

from pyetm.clients.base_client import BaseClient
from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult


class FetchUserCollectionsRunner(BaseRunner[List[Dict[str, Any]]]):
    """
    Runner for fetching all collections belonging to the authenticated user.

    GET /api/v3/collections

    Returns all collections the user has access to in a single request.
    """

    @staticmethod
    def run(
        client: BaseClient,
        **kwargs: Any,
    ) -> ServiceResult[List[Dict[str, Any]]]:
        """
        Fetch all collections for the authenticated user.

        Args:
            client: HTTP client with authentication
            **kwargs: Additional query parameters

        Returns:
            ServiceResult with list of collection data dictionaries
        """
        result = FetchUserCollectionsRunner._make_request(
            client=client,
            method="get",
            path="/collections",
            payload=kwargs if kwargs else None,
        )

        if not result.success:
            return result

        if result.data is None:
            return ServiceResult.fail(["No data returned from API"])

        # Handle paginated response with 'collections' key
        if isinstance(result.data, dict) and "collections" in result.data:
            data = result.data.get("collections", [])
            if not isinstance(data, list):
                return ServiceResult.fail(
                    [f"Expected 'collections' key to contain list, got {type(data).__name__}"]
                )
            return ServiceResult.ok(data=data)

        # Handle direct list response (backwards compatibility)
        if isinstance(result.data, list):
            return ServiceResult.ok(data=result.data)

        # Invalid response type
        return ServiceResult.fail(
            [f"Expected list or paginated response, got {type(result.data).__name__}"]
        )
