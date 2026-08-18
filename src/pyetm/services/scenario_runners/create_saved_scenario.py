"""Service for creating saved scenarios."""

from typing import Any

from pyetm.clients.base_client import BaseClient
from pyetm.config.api_compat import saved_scenario_payload
from pyetm.services.scenario_runners.base_runner import BaseRunner

from ..service_result import ServiceResult


class CreateSavedScenarioRunner(BaseRunner[dict[str, Any]]):
    """Runner for creating a SavedScenario in MyETM from a SessionID scenario.

    POST /api/v3/saved_scenarios
    """

    REQUIRED_KEYS = ["scenario_id", "title"]
    OPTIONAL_KEYS = ["private"]

    @staticmethod
    def run(
        client: BaseClient, saved_scenario_data: dict[str, Any], **kwargs: Any
    ) -> ServiceResult[dict[str, Any]]:
        """Create a new SavedScenario in MyETM.

        Args:
            client: The HTTP client to use
            saved_scenario_data: Dictionary with scenario_id, title, private
            **kwargs: Additional arguments passed to the request

        Example usage:
            result = CreateSavedScenarioRunner.run(
                client=client,
                saved_scenario_data={
                    "scenario_id": 123,
                    "title": "My Saved Scenario",
                    "private": False
                }
            )
        """
        errors = CreateSavedScenarioRunner._validate_required_fields(
            saved_scenario_data, CreateSavedScenarioRunner.REQUIRED_KEYS
        )

        if errors:
            return ServiceResult.fail(errors)

        all_allowed = (
            CreateSavedScenarioRunner.REQUIRED_KEYS + CreateSavedScenarioRunner.OPTIONAL_KEYS
        )
        filtered_data, warnings = CreateSavedScenarioRunner._filter_allowed_fields(
            saved_scenario_data,
            all_allowed,
            "create saved scenario",
        )

        payload = saved_scenario_payload(filtered_data, client.session.base_url)

        result = CreateSavedScenarioRunner._make_request(
            client=client,
            method="post",
            path="/saved_scenarios",
            payload=payload,
            **kwargs,
        )

        if result.success and warnings:
            combined_errors = list(result.errors) + warnings
            assert result.data is not None, "Success result must have data"
            return ServiceResult.ok(data=result.data, errors=combined_errors)

        return result
