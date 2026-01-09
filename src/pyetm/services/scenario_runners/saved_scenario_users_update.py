from typing import Any, Dict, List
from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult
from pyetm.clients.base_client import BaseClient


class SavedScenarioUsersUpdateRunner(BaseRunner[List[Dict[str, Any]]]):
    """
    Runner for updating user roles on a SavedScenario.

    PUT /api/v3/saved_scenarios/:saved_scenario_id/users
    """

    REQUIRED_KEYS = ["role"]

    @staticmethod
    def run(
        client: BaseClient,
        saved_scenario_id: int,
        users: List[Dict[str, Any]],
        **kwargs,
    ) -> ServiceResult[List[Dict[str, Any]]]:
        if not users:
            return ServiceResult.fail(["No users provided"])

        errors = []
        for i, user in enumerate(users):
            missing = SavedScenarioUsersUpdateRunner._validate_required_fields(
                user, SavedScenarioUsersUpdateRunner.REQUIRED_KEYS
            )
            if missing:
                errors.extend([f"User {i}: {err}" for err in missing])

            if "user_id" not in user and "user_email" not in user:
                errors.append(
                    f"User {i}: Must provide either 'user_id' or 'user_email'"
                )

        if errors:
            return ServiceResult.fail(errors)

        payload = {"saved_scenario_users": users}

        return SavedScenarioUsersUpdateRunner._make_request(
            client=client,
            method="put",
            path=f"/saved_scenarios/{saved_scenario_id}/users",
            payload=payload,
            **kwargs,
        )
