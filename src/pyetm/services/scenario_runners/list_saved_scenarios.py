from typing import Any, Dict, List
from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult
from pyetm.clients.base_client import BaseClient


class ListSavedScenariosRunner(BaseRunner[List[Dict[str, Any]]]):
    """
    Runner for listing all SavedScenarios accessible to the current user.

    GET /api/v3/saved_scenarios
    """

    @staticmethod
    def run(
        client: BaseClient, page: int = 1, limit: int = 25, **kwargs
    ) -> ServiceResult[List[Dict[str, Any]]]:
        """
        List all SavedScenarios for the current user.

        Args:
            client: HTTP client
            page: Page number (default: 1)
            limit: Items per page (default: 25, max: 100)

        Returns:
            ServiceResult with list of SavedScenario data
        """
        params = {"page": page, "limit": min(limit, 100)}

        result = ListSavedScenariosRunner._make_request(
            client=client, method="get", path="/saved_scenarios", params=params, **kwargs
        )

        if not result.success:
            return result

        response_data = result.data or {}

        if not isinstance(response_data, dict) or 'data' not in response_data:
            return ServiceResult.fail(
                ["Expected wrapped response with 'data' key from /saved_scenarios endpoint"]
            )

        return ServiceResult.ok(data=response_data['data'], errors=result.errors)
