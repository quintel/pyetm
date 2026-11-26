"""Runner for adding a saved scenario's users in MyETM."""

from typing import Any, Dict, List

from pyetm.clients.base_client import BaseClient
from pyetm.services.scenario_runners.base_runner import BaseRunner
from pyetm.services.scenario_runners.saved_scenario_users_index import batch_outcome
from ..service_result import ServiceResult


class SavedScenarioUsersCreateRunner(BaseRunner[List[Dict[str, Any]]]):
    """
    Adds users, each given as {"email": ..., "role": ...}. Fails naming every refused user.

    POST /api/v4/saved_scenarios/{id}/users
    """

    REQUIRED_KEYS = ['email', 'role']

    @staticmethod
    def run(
        client: BaseClient, saved_scenario_id: int, users: List[Dict[str, Any]], **kwargs: Any
    ) -> ServiceResult[List[Dict[str, Any]]]:
        errors = [
            f"User {i}: {error}"
            for i, user in enumerate(users)
            for error in SavedScenarioUsersCreateRunner._validate_required_fields(user, SavedScenarioUsersCreateRunner.REQUIRED_KEYS)
        ]
        if not users or errors:
            return ServiceResult.fail(errors or ["No users provided"])

        return batch_outcome(
            SavedScenarioUsersCreateRunner._make_request(
                client,
                "post",
                f"/saved_scenarios/{saved_scenario_id}/users",
                payload={"saved_scenario_users": users},
                **kwargs,
            )
        )
