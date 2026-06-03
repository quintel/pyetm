"""Service for permanently deleting a SavedScenario and its underlying Session (hard delete with cascade)."""

from typing import Any, Dict
from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult
from pyetm.clients.base_client import BaseClient


class DestroySavedScenarioRunner(BaseRunner[Dict[str, Any]]):
    """
    Runner for permanently deleting a SavedScenario AND its underlying ETEngine scenario (hard delete with cascade).

    DELETE /api/v3/saved_scenarios/:id (SavedScenario)
    DELETE /api/v3/scenarios/:id (Session)

    WARNING: This is a permanent deletion and cannot be undone. Both the SavedScenario
    and its underlying Session will be permanently removed.
    """

    @staticmethod
    def run(
        client: BaseClient,
        saved_scenario_id: int,
        scenario_id: int,
        **kwargs: Any,
    ) -> ServiceResult[Dict[str, Any]]:
        """
        Permanently delete a SavedScenario and its underlying Session (hard delete with cascade).

        WARNING: This is a permanent deletion and cannot be undone. Both the SavedScenario
        in MyETM and the underlying Session in ETEngine will be permanently removed.

        Args:
            client: The HTTP client to use
            saved_scenario_id: ID of the SavedScenario to permanently delete
            scenario_id: ID of the underlying ETEngine scenario to permanently delete
            **kwargs: Additional arguments passed to the request

        Returns:
            ServiceResult with deletion confirmation data

        Example usage:
            result = DestroySavedScenarioRunner.run(
                client=client,
                saved_scenario_id=123,
                scenario_id=456
            )
            if result.success:
                print("SavedScenario and Session permanently deleted")
        """
        if not isinstance(saved_scenario_id, int) or saved_scenario_id <= 0:
            return ServiceResult.fail([f"Invalid saved_scenario_id: {saved_scenario_id}. Must be a positive integer."])

        if not isinstance(scenario_id, int) or scenario_id <= 0:
            return ServiceResult.fail([f"Invalid scenario_id: {scenario_id}. Must be a positive integer."])

        # First, delete the SavedScenario from MyETM
        saved_scenario_result = DestroySavedScenarioRunner._make_request(
            client=client,
            method="delete",
            path=f"/saved_scenarios/{saved_scenario_id}",
            **kwargs,
        )

        if not saved_scenario_result.success:
            return ServiceResult.fail(
                [f"Failed to delete SavedScenario: {saved_scenario_result.errors}"]
            )

        # Then, delete the underlying Session from ETEngine
        session_result = DestroySavedScenarioRunner._make_request(
            client=client,
            method="delete",
            path=f"/scenarios/{scenario_id}",
            **kwargs,
        )

        if not session_result.success:
            return ServiceResult.fail(
                [f"SavedScenario deleted but failed to delete Session: {session_result.errors}"]
            )

        return ServiceResult.ok({
            "message": "SavedScenario and underlying Session permanently deleted",
            "saved_scenario_id": saved_scenario_id,
            "scenario_id": scenario_id
        })
