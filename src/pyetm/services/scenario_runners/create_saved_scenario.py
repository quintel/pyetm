from typing import Any, Dict
from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult
from pyetm.clients.base_client import BaseClient


class CreateSavedScenarioRunner(BaseRunner[Dict[str, Any]]):
    """
    Runner for creating a SavedScenario in MyETM from a SessionID scenario.

    POST /api/v3/saved_scenarios

    Args:
        client: The HTTP client to use
        saved_scenario_data: Dictionary with scenario_id, title, description, private
        **kwargs: Additional arguments passed to the request
    """

    REQUIRED_KEYS = ["scenario_id", "title"]
    OPTIONAL_KEYS = ["description", "private"]

    @staticmethod
    def run(
        client: BaseClient, saved_scenario_data: Dict[str, Any], **kwargs
    ) -> ServiceResult[Dict[str, Any]]:
        """
        Create a new SavedScenario in MyETM.

        Example usage:
            result = CreateSavedScenarioRunner.run(
                client=client,
                saved_scenario_data={
                    "scenario_id": 123,
                    "title": "My Saved Scenario",
                    "description": "Optional description",
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
            CreateSavedScenarioRunner.REQUIRED_KEYS
            + CreateSavedScenarioRunner.OPTIONAL_KEYS
        )
        filtered_data, warnings = CreateSavedScenarioRunner._filter_allowed_fields(
            saved_scenario_data,
            all_allowed,
            "create saved scenario",
        )

        payload = {"saved_scenario": filtered_data}

        result = CreateSavedScenarioRunner._make_request(
            client=client,
            method="post",
            path="/saved_scenarios",
            payload=payload,
            **kwargs,
        )

        if result.success and warnings:
            combined_errors = list(result.errors) + warnings
            return ServiceResult.ok(data=result.data, errors=combined_errors)

        return result
