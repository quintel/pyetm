"""Runner for updating a saved scenario in MyETM."""

from typing import Any

from pyetm.clients.base_client import BaseClient
from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult


class UpdateSavedScenarioRunner(BaseRunner[dict[str, Any]]):
    """
    Updates a saved scenario's MyETM record.

    PUT /api/v4/saved_scenarios/{id}
    """

    ALLOWED_KEYS = ["title", "description", "private"]

    @staticmethod
    def run(
        client: BaseClient, saved_scenario_id: int, update_data: dict[str, Any], **kwargs: Any
    ) -> ServiceResult[dict[str, Any]]:
        filtered_data, warnings = UpdateSavedScenarioRunner._filter_allowed_fields(
            update_data, UpdateSavedScenarioRunner.ALLOWED_KEYS, "update saved scenario"
        )
        if not filtered_data:
            return ServiceResult.fail(["No valid fields provided for update"] + warnings)

        result = UpdateSavedScenarioRunner._make_myetm_request(
            client,
            "put",
            f"/saved_scenarios/{saved_scenario_id}",
            payload={"saved_scenario": filtered_data},
            **kwargs,
        )
        return result.with_warnings(warnings)
