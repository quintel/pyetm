"""Runner for creating a saved scenario in MyETM."""

from typing import Any

from pyetm.clients.base_client import BaseClient
from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult


class CreateSavedScenarioRunner(BaseRunner[dict[str, Any]]):
    """
    Creates a saved scenario, whose Session MyETM creates: fresh from an area_code and end_year,
    or from a source saved scenario, interpolated when the end_year differs from the source's.

    POST /api/v4/saved_scenarios
    """

    REQUIRED_KEYS = ["title", "version"]
    OPTIONAL_KEYS = ["area_code", "end_year", "source", "private", "description"]

    @staticmethod
    def run(client: BaseClient, saved_scenario_data: dict[str, Any], **kwargs: Any) -> ServiceResult[dict[str, Any]]:
        errors = CreateSavedScenarioRunner._validate_required_fields(
            saved_scenario_data, CreateSavedScenarioRunner.REQUIRED_KEYS
        )
        if errors:
            return ServiceResult.fail(errors)

        filtered_data, warnings = CreateSavedScenarioRunner._filter_allowed_fields(
            saved_scenario_data,
            CreateSavedScenarioRunner.REQUIRED_KEYS + CreateSavedScenarioRunner.OPTIONAL_KEYS,
            "create saved scenario",
        )
        result = CreateSavedScenarioRunner._make_myetm_request(
            client, "post", "/saved_scenarios", payload={"saved_scenario": filtered_data}, **kwargs
        )
        return result.with_warnings(warnings)
