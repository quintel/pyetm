from typing import Any, Dict
from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult
from pyetm.clients.base_client import BaseClient


class UpdateSavedScenarioRunner(BaseRunner[Dict[str, Any]]):
    """
    Runner for updating a SavedScenario in MyETM.

    PUT /api/v3/saved_scenarios/:id

    Args:
        client: The HTTP client to use
        saved_scenario_id: ID of the SavedScenario to update
        update_data: Dictionary with fields to update (title, description, private, discarded)
        **kwargs: Additional arguments passed to the request
    """

    ALLOWED_KEYS = ["title", "scenario_id", "private", "discarded"]

    @staticmethod
    def run(
        client: BaseClient,
        saved_scenario_id: int,
        update_data: Dict[str, Any],
        **kwargs,
    ) -> ServiceResult[Dict[str, Any]]:
        """
        Update an existing SavedScenario in MyETM.

        Example usage:
            result = UpdateSavedScenarioRunner.run(
                client=client,
                saved_scenario_id=123,
                update_data={
                    "title": "Updated Title",
                    "description": "New description"
                }
            )
        """
        if not update_data:
            return ServiceResult.fail(["No fields provided for update"])

        filtered_data, warnings = UpdateSavedScenarioRunner._filter_allowed_fields(
            update_data,
            UpdateSavedScenarioRunner.ALLOWED_KEYS,
            "update saved scenario",
        )

        if not filtered_data:
            return ServiceResult.fail(
                ["No valid fields provided for update"] + warnings
            )

        payload = {"saved_scenario": filtered_data}

        result = UpdateSavedScenarioRunner._make_request(
            client=client,
            method="put",
            path=f"/saved_scenarios/{saved_scenario_id}",
            payload=payload,
            **kwargs,
        )

        if result.success and warnings:
            combined_errors = list(result.errors) + warnings
            return ServiceResult.ok(data=result.data, errors=combined_errors)

        return result
