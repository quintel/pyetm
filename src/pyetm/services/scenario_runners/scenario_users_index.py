from typing import Any, Dict, List
from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult
from pyetm.clients.base_client import BaseClient


class ScenarioUsersIndexRunner(BaseRunner[List[Dict[str, Any]]]):
    """
    Runner for fetching all users with access to a Scenario.

    GET /api/v3/scenarios/:scenario_id/users
    """

    @staticmethod
    def run(
        client: BaseClient, scenario_id: int, **kwargs
    ) -> ServiceResult[List[Dict[str, Any]]]:
        return ScenarioUsersIndexRunner._make_request(
            client=client,
            method="get",
            path=f"/scenarios/{scenario_id}/users",
            **kwargs,
        )
