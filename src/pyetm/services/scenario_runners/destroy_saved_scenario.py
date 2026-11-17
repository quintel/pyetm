"""Service for permanently deleting a SavedScenario in MyETM."""

from typing import Any, Dict
from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult
from pyetm.clients.base_client import BaseClient


class DestroySavedScenarioRunner(BaseRunner[Dict[str, Any]]):
    """
    Runner for permanently deleting a SavedScenario.

    DELETE /api/v3/saved_scenarios/:id

    MyETM purges the SavedScenario and unbinds its Session, which then expires on its own.
    WARNING: This is a permanent deletion and cannot be undone.
    """

    @staticmethod
    def run(
        client: BaseClient,
        saved_scenario_id: int,
        **kwargs: Any,
    ) -> ServiceResult[Dict[str, Any]]:
        """
        Permanently delete a SavedScenario.

        Args:
            client: The HTTP client to use
            saved_scenario_id: ID of the SavedScenario to permanently delete
            **kwargs: Additional arguments passed to the request

        Returns:
            ServiceResult with deletion confirmation data
        """
        if not isinstance(saved_scenario_id, int) or saved_scenario_id <= 0:
            return ServiceResult.fail(
                [f"Invalid saved_scenario_id: {saved_scenario_id}. Must be a positive integer."]
            )

        result = DestroySavedScenarioRunner._make_request(
            client=client,
            method="delete",
            path=f"/saved_scenarios/{saved_scenario_id}",
            **kwargs,
        )

        if not result.success:
            return ServiceResult.fail([f"Failed to delete SavedScenario: {result.errors}"])

        return ServiceResult.ok(
            {"message": "SavedScenario permanently deleted", "saved_scenario_id": saved_scenario_id}
        )
