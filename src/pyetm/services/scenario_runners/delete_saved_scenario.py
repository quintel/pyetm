"""Service for discarding a saved scenario (soft-delete)."""

from typing import Any, Dict
from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult
from pyetm.clients.base_client import BaseClient


class DeleteSavedScenarioRunner(BaseRunner[Dict[str, Any]]):
    """
    Runner for discarding a SavedScenario from MyETM (soft-delete).

    PUT /api/v3/saved_scenarios/:id/discard
    """

    @staticmethod
    def build_request(saved_scenario_id: int) -> Dict[str, Any]:
        """
        Build discard request for concurrent batching.

        Args:
            saved_scenario_id: ID of the SavedScenario to discard

        Returns:
            Request dict ready for AsyncBatchRunner
        """
        return {
            "method": "put",
            "path": f"/saved_scenarios/{saved_scenario_id}/discard",
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
        Discard a SavedScenario from MyETM (soft-delete).

        Args:
            client: The HTTP client to use
            saved_scenario_id: ID of the SavedScenario to discard
            **kwargs: Additional arguments passed to the request

        Returns:
            ServiceResult with discard confirmation data

        Example usage:
            result = DeleteSavedScenarioRunner.run(
                client=client,
                saved_scenario_id=123
            )
            if result.success:
                print("Scenario discarded successfully")
        """
        if not isinstance(saved_scenario_id, int) or saved_scenario_id <= 0:
            return ServiceResult.fail([f"Invalid saved_scenario_id: {saved_scenario_id}. Must be a positive integer."])

        result = DeleteSavedScenarioRunner._make_request(
            client=client,
            method="put",
            path=f"/saved_scenarios/{saved_scenario_id}/discard",
            **kwargs,
        )

        return result
