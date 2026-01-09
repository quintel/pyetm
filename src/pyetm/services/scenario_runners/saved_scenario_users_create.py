from typing import Any, Dict, List
from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult
from pyetm.clients.base_client import BaseClient


class SavedScenarioUsersCreateRunner(BaseRunner[List[Dict[str, Any]]]):
    """
    Runner for adding users to a SavedScenario.

    POST /api/v3/saved_scenarios/:saved_scenario_id/users

    Args:
        client: The HTTP client to use
        saved_scenario_id: ID of the SavedScenario
        users: List of user objects to add, each containing:
            - user_email: Email address (required)
            - role: User role - scenario_owner, scenario_collaborator, or scenario_viewer (required)
            - user_id: ID of existing user (optional, will be auto-coupled if email matches)
        **kwargs: Additional arguments passed to the request

    Returns:
        ServiceResult containing list of created user objects

    Note:
        When a user is added to a saved scenario, they are also automatically added to:
        - The current scenario (via scenario_id)
        - All historical scenarios (via scenario_id_history)
    """

    REQUIRED_KEYS = ["user_email", "role"]

    @staticmethod
    def run(
        client: BaseClient,
        saved_scenario_id: int,
        users: List[Dict[str, Any]],
        **kwargs,
    ) -> ServiceResult[List[Dict[str, Any]]]:
        """
        Add users to a saved scenario with specified roles.
        """
        if not users:
            return ServiceResult.fail(["No users provided"])

        errors = []
        for i, user in enumerate(users):
            missing = SavedScenarioUsersCreateRunner._validate_required_fields(
                user, SavedScenarioUsersCreateRunner.REQUIRED_KEYS
            )
            if missing:
                errors.extend([f"User {i}: {err}" for err in missing])

        if errors:
            return ServiceResult.fail(errors)

        payload = {"saved_scenario_users": users}

        return SavedScenarioUsersCreateRunner._make_request(
            client=client,
            method="post",
            path=f"/saved_scenarios/{saved_scenario_id}/users",
            payload=payload,
            **kwargs,
        )
