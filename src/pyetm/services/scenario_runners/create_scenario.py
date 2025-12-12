from typing import Any, Dict
from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult
from pyetm.clients.base_client import BaseClient


class CreateScenarioRunner(BaseRunner[Dict[str, Any]]):
    """
    Runner for creating a new scenario.

    POST /api/v3/scenarios

    Args:
        client: The HTTP client to use
        scenario_data: Dictionary of scenario attributes for creation
        **kwargs: Additional arguments passed to the request
    """

    # Required fields for scenario creation
    REQUIRED_KEYS = [
        "area_code",
        "end_year",
    ]

    # Optional fields that can be set during creation
    OPTIONAL_KEYS = [
        "keep_compatible",
        "private",
        "source",
        "title",
        "metadata",
        "start_year",
        "scaling",
        "template",
        "url",
    ]

    @staticmethod
    def run(
        client: BaseClient, scenario_data: Dict[str, Any], **kwargs
    ) -> ServiceResult[Dict[str, Any]]:
        """
        Create a new scenario.

        Example usage:
            result = CreateScenarioRunner.run(
                client=client,
                scenario_data={
                    "area_code": "nl",
                    "end_year": 2050,
                    "private": True,
                    "metadata": {"description": "My new scenario"}
                }
            )
        """
        # Validate required fields
        missing_required = []
        for key in CreateScenarioRunner.REQUIRED_KEYS:
            if key not in scenario_data:
                missing_required.append(key)

        if missing_required:
            return ServiceResult.fail(
                [f"Missing required fields: {', '.join(missing_required)}"]
            )

        # Filter to only allowed fields
        all_allowed = (
            CreateScenarioRunner.REQUIRED_KEYS + CreateScenarioRunner.OPTIONAL_KEYS
        )
        filtered_data = {
            key: value for key, value in scenario_data.items() if key in all_allowed
        }

        warnings = []
        filtered_keys = set(scenario_data.keys()) - set(filtered_data.keys())
        for key in filtered_keys:
            warnings.append(f"Ignoring invalid field for scenario creation: {key!r}")

        if "template" in filtered_data:
            filtered_data["preset_scenario_id"] = filtered_data.pop("template")

        payload = {"scenario": filtered_data}

        result = CreateScenarioRunner._make_request(
            client=client,
            method="post",
            path="/scenarios",
            payload=payload,
        )

        if result.success and warnings:
            # Merge our warnings with any from the API call
            combined_errors = list(result.errors) + warnings
            return ServiceResult.ok(data=result.data, errors=combined_errors)

        return result
