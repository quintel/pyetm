"""Service for fetching user saved scenarios (MyETM)."""

from typing import Any, Dict, List

from pyetm.clients.base_client import BaseClient
from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult


class FetchUserSavedScenariosRunner(BaseRunner[List[Dict[str, Any]]]):
    """
    Runner for fetching all saved scenarios belonging to the authenticated user.

    GET /api/v3/saved_scenarios

    Returns all saved scenarios the user has access to (owned or shared) in a
    single request (not paginated).
    """

    @staticmethod
    def run(
        client: BaseClient,
        **kwargs: Any,
    ) -> ServiceResult[List[Dict[str, Any]]]:
        """
        Fetch all saved scenarios for the authenticated user.

        Args:
            client: HTTP client with authentication
            **kwargs: Additional query parameters

        Returns:
            ServiceResult with list of saved scenario data dictionaries
        """
        result = FetchUserSavedScenariosRunner._make_request(
            client=client,
            method="get",
            path="/saved_scenarios",
            payload=kwargs if kwargs else None,
        )

        if not result.success:
            return result

        if result.data is None:
            return ServiceResult.fail(["No data returned from API"])

        # Handle paginated response with 'data' key
        if isinstance(result.data, dict) and "data" in result.data:
            data = result.data.get("data", [])
            if not isinstance(data, list):
                return ServiceResult.fail(
                    [f"Expected 'data' key to contain list, got {type(data).__name__}"]
                )
            return ServiceResult.ok(data=data)

        # Handle direct list response (backwards compatibility)
        if isinstance(result.data, list):
            return ServiceResult.ok(data=result.data)

        # Invalid response type
        return ServiceResult.fail(
            [f"Expected list or paginated response, got {type(result.data).__name__}"]
        )
