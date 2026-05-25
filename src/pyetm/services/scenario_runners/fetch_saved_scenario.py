"""Service for fetch saved scenario operations."""

from typing import Dict, Any
from pyetm.services.scenario_runners.base_runner import BaseRunner, ScenarioIdentifier
from ..service_result import ServiceResult
from pyetm.clients.base_client import BaseClient


class FetchSavedScenarioRunner(BaseRunner[Dict[str, Any]]):
    """
    Runner for fetching a SavedScenario from MyETM by its ID.

    GET /api/v3/saved_scenarios/{saved_scenario_id}
    """

    REQUIRED_KEYS = [
        "id",
        "scenario_id",
        "title",
        "private",
        "created_at",
        "updated_at",
    ]

    @staticmethod
    def run(
        client: BaseClient, saved_scenario: ScenarioIdentifier, **kwargs: Any
    ) -> ServiceResult[Dict[str, Any]]:
        """
        Fetch a single SavedScenario by ID.

        Args:
            client: HTTP client
            saved_scenario: Object with an 'id' attribute

        Returns:
            ServiceResult with SavedScenario data
        """
        result = FetchSavedScenarioRunner._make_request(
            client=client, method="get", path=f"/saved_scenarios/{saved_scenario.id}"
        )

        if not result.success:
            for error in result.errors:
                if "404" in error or "not found" in error.lower():
                    return ServiceResult.fail(
                        [
                            f"SavedScenario {saved_scenario.id} not found on this environment"
                        ]
                    )
            return result

        if result.data is None:
            return ServiceResult.fail(["No data returned from API"])

        # Check if the API returned an error payload in a successful response
        # MyETM API sometimes returns 200 OK with {"errors": [...], "scenario": null}
        if isinstance(result.data, dict) and "errors" in result.data:
            errors = result.data.get("errors", [])
            if errors:
                # Check for "not found" errors
                for error in errors:
                    if isinstance(error, str) and "not found" in error.lower():
                        return ServiceResult.fail(
                            [f"SavedScenario {saved_scenario.id} not found on this environment"]
                        )
                # Return generic failure with the error messages
                return ServiceResult.fail(errors if isinstance(errors, list) else [str(errors)])

        _, warnings = FetchSavedScenarioRunner._validate_response_keys(
            result.data,
            FetchSavedScenarioRunner.REQUIRED_KEYS,
            fill_missing=False,
        )

        return ServiceResult.ok(data=result.data, errors=warnings)
