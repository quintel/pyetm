"""Runner for saving a scenario as a Snapshot in MyETM."""

from typing import Any, Dict

from pyetm.clients.base_client import BaseClient
from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult


class SaveSnapshotRunner(BaseRunner[Dict[str, Any]]):
    """
    Saves a scenario: its current Session becomes a Snapshot, and a clone of it becomes current.

    POST /api/v4/saved_scenarios/{id}/snapshots
    """

    @staticmethod
    def run(client: BaseClient, saved_scenario_id: int, **kwargs: Any) -> ServiceResult[Dict[str, Any]]:
        return SaveSnapshotRunner._make_myetm_request(
            client, "post", f"/saved_scenarios/{saved_scenario_id}/snapshots", **kwargs
        )
