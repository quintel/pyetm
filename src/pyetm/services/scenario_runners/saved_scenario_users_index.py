from typing import Any, Dict, List
from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult
from pyetm.clients.base_client import BaseClient


class SavedScenarioUsersIndexRunner(BaseRunner[List[Dict[str, Any]]]):
    """
    Runner for fetching all users with access to a SavedScenario.

    GET /api/v3/saved_scenarios/:saved_scenario_id/users
    """

    @staticmethod
    def run(
        client: BaseClient, saved_scenario_id: int, **kwargs
    ) -> ServiceResult[List[Dict[str, Any]]]:
        return SavedScenarioUsersIndexRunner._make_request(
            client=client,
            method="get",
            path=f"/saved_scenarios/{saved_scenario_id}/users",
            **kwargs,
        )
