from pyetm.services.scenario_runners.base_runner import BaseRunner, ScenarioIdentifier
from ..service_result import ServiceResult
from pyetm.clients.base_client import BaseClient


class DeleteSavedScenarioRunner(BaseRunner[None]):
    """
    Runner for deleting a SavedScenario from MyETM.

    DELETE /api/v3/saved_scenarios/{saved_scenario_id}

    Note: This does NOT delete the underlying SessionID scenario in ETEngine.
    """

    @staticmethod
    def run(client: BaseClient, saved_scenario: ScenarioIdentifier, **kwargs) -> ServiceResult[None]:
        """
        Delete a SavedScenario.

        Args:
            client: HTTP client
            saved_scenario: Object with an 'id' attribute

        Returns:
            ServiceResult indicating success or failure
        """
        return DeleteSavedScenarioRunner._make_request(
            client=client,
            method="delete",
            path=f"/saved_scenarios/{saved_scenario.id}",
            **kwargs
        )
