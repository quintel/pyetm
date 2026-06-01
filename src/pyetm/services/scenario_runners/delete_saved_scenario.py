"""Service for deleting a saved scenario."""

from typing import Any, Dict
from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult
from pyetm.clients.base_client import BaseClient


class DeleteSavedScenarioRunner(BaseRunner[Dict[str, Any]]):
    """
    Runner for deleting a SavedScenario from MyETM.

    DELETE /api/v3/saved_scenarios/:id
    """

    @staticmethod
    def build_request(saved_scenario_id: int) -> Dict[str, Any]:
        """
        Build delete request for concurrent batching.

        Args:
            saved_scenario_id: ID of the SavedScenario to delete

        Returns:
            Request dict ready for AsyncBatchRunner
        """
        return {
            "method": "delete",
            "path": f"/saved_scenarios/{saved_scenario_id}",
            "payload": None,
            "kwargs": {},
        }

    @staticmethod
    def run(
        client: BaseClient,
        saved_scenario_id: int,
        **kwargs: Any,
    ) -> ServiceResult[Dict[str, Any]]:
        """
        Delete a SavedScenario from MyETM.

        Args:
            client: The HTTP client to use
            saved_scenario_id: ID of the SavedScenario to delete
            **kwargs: Additional arguments passed to the request

        Returns:
            ServiceResult with deletion confirmation data

        Example usage:
            result = DeleteSavedScenarioRunner.run(
                client=client,
                saved_scenario_id=123
            )
            if result.success:
                print("Scenario deleted successfully")
        """
        if not isinstance(saved_scenario_id, int) or saved_scenario_id <= 0:
            return ServiceResult.fail([f"Invalid saved_scenario_id: {saved_scenario_id}. Must be a positive integer."])

        result = DeleteSavedScenarioRunner._make_request(
            client=client,
            method="delete",
            path=f"/saved_scenarios/{saved_scenario_id}",
            **kwargs,
        )

        return result
