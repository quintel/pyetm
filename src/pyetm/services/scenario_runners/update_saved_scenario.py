from typing import Dict, Any
from pyetm.services.scenario_runners.base_runner import BaseRunner, ScenarioIdentifier
from ..service_result import ServiceResult
from pyetm.clients.base_client import BaseClient


class UpdateSavedScenarioRunner(BaseRunner[Dict[str, Any]]):
    """
    Runner for updating a SavedScenario's metadata in MyETM.

    PUT /api/v3/saved_scenarios/{saved_scenario_id}
    """

    UPDATABLE_KEYS = ["title", "description", "private"]

    @staticmethod
    def run(
        client: BaseClient,
        saved_scenario: ScenarioIdentifier,
        update_data: Dict[str, Any],
        **kwargs,
    ) -> ServiceResult[Dict[str, Any]]:
        """
        Update a SavedScenario's metadata.

        Args:
            client: HTTP client
            saved_scenario: Object with an 'id' attribute
            update_data: Dictionary with fields to update

        Returns:
            ServiceResult with updated SavedScenario data
        """
        filtered_data, warnings = UpdateSavedScenarioRunner._filter_allowed_fields(
            update_data,
            UpdateSavedScenarioRunner.UPDATABLE_KEYS,
            "update saved scenario",
        )

        if not filtered_data:
            return ServiceResult.fail(["No valid fields to update"])

        result = UpdateSavedScenarioRunner._make_request(
            client=client,
            method="put",
            path=f"/saved_scenarios/{saved_scenario.id}",
            payload=filtered_data,
            **kwargs,
        )

        if result.success and warnings:
            combined_errors = list(result.errors) + warnings
            return ServiceResult.ok(data=result.data, errors=combined_errors)

        return result
