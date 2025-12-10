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
        client: BaseClient,
        saved_scenario: ScenarioIdentifier,
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
            return result

        _, warnings = FetchSavedScenarioRunner._validate_response_keys(
            result.data,
            FetchSavedScenarioRunner.REQUIRED_KEYS,
            fill_missing=False
        )

        return ServiceResult.ok(data=result.data, errors=warnings)
