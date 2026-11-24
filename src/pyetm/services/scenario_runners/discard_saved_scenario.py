"""Runner for discarding a saved scenario in MyETM."""

from typing import Any, Dict

from pyetm.clients.base_client import BaseClient
from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult


class DiscardSavedScenarioRunner(BaseRunner[Dict[str, Any]]):
    """
    Discards a saved scenario, which MyETM keeps recoverable for 60 days.

    PUT /api/v4/saved_scenarios/{id}/discard
    """

    @staticmethod
    def build_request(saved_scenario_id: int) -> Dict[str, Any]:
        """The request, for discarding many concurrently."""
        return {"method": "put", "path": f"/saved_scenarios/{saved_scenario_id}/discard"}

    @staticmethod
    def run(client: BaseClient, saved_scenario_id: int, **kwargs: Any) -> ServiceResult[Dict[str, Any]]:
        return DiscardSavedScenarioRunner._make_myetm_request(
            client, "put", f"/saved_scenarios/{saved_scenario_id}/discard", **kwargs
        )
