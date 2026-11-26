"""Runner for permanently deleting a saved scenario in MyETM."""

from typing import Any

from pyetm.clients.base_client import BaseClient
from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult


class DestroySavedScenarioRunner(BaseRunner[Any]):
    """
    Permanently deletes a saved scenario. MyETM unbinds its Sessions.

    DELETE /api/v4/saved_scenarios/{id}
    """

    @staticmethod
    def run(client: BaseClient, saved_scenario_id: int, **kwargs: Any) -> ServiceResult[Any]:
        return DestroySavedScenarioRunner._make_request(
            client, "delete", f"/saved_scenarios/{saved_scenario_id}", **kwargs
        )
