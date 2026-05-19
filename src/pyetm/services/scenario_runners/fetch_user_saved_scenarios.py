from typing import Any, Dict, List
from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult
from pyetm.clients.base_client import BaseClient


class FetchUserSavedScenariosRunner(BaseRunner[List[Dict[str, Any]]]):
    """
    Runner for fetching the authenticated user's saved scenarios (MyETM).

    GET /api/v3/saved_scenarios
    """

    @staticmethod
    def run(
        client: BaseClient,
    ) -> ServiceResult[List[Dict[str, Any]]]:
        """
        Fetch a list of saved scenarios for the authenticated user.

        Args:
            client: HTTP client

        Returns:
            ServiceResult with list of saved scenario data dicts
        """
        result = FetchUserSavedScenariosRunner._make_request(
            client=client,
            method="get",
            path="/saved_scenarios",
            payload=None,
        )

        if not result.success:
            return result

        return ServiceResult.ok(data=result.data.get("data", []))
