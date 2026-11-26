"""Runner for fetching a saved scenario from MyETM."""

from typing import Any, Dict

from pyetm.clients.base_client import BaseClient
from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult


class FetchSavedScenarioRunner(BaseRunner[Dict[str, Any]]):
    """
    Fetches a saved scenario's MyETM record.

    GET /api/v4/saved_scenarios/{id}
    """

    @staticmethod
    def run(client: BaseClient, saved_scenario_id: int, **kwargs: Any) -> ServiceResult[Dict[str, Any]]:
        return FetchSavedScenarioRunner._make_myetm_request(
            client, "get", f"/saved_scenarios/{saved_scenario_id}", **kwargs
        )
