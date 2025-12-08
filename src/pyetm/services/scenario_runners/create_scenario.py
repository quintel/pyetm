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
        errors = CreateScenarioRunner._validate_required_fields(
            scenario_data, CreateScenarioRunner.REQUIRED_KEYS
        )

        if errors:
            return ServiceResult.fail(errors)

        all_allowed = (
            CreateScenarioRunner.REQUIRED_KEYS + CreateScenarioRunner.OPTIONAL_KEYS
        )
        filtered_data, warnings = CreateScenarioRunner._filter_allowed_fields(
            scenario_data,
            all_allowed,
            "create scenario",
        )

        payload = {"scenario": filtered_data}

        result = CreateScenarioRunner._make_request(
            client=client, method="post", path="/scenarios", payload=payload, **kwargs
        )

        if result.success and warnings:
            combined_errors = list(result.errors) + warnings
            return ServiceResult.ok(data=result.data, errors=combined_errors)

        return result
