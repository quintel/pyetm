"""Runners for a saved scenario's users in MyETM."""

from typing import Any, Dict, List, cast

from pyetm.clients.base_client import BaseClient
from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult


def batch_outcome(result: ServiceResult[Any]) -> ServiceResult[List[Dict[str, Any]]]:
    """A v4 batch answers 207 whatever happened, so a refused item becomes a failure here."""
    if not result.success:
        return result
    items = cast(Dict[str, Any], result.data)["data"]
    refused = [f"{item.get('code')}: {item.get('detail')}" for item in items if item.get("status") != "ok"]
    return ServiceResult.fail(refused) if refused else ServiceResult.ok([item["data"] for item in items])


class SavedScenarioUsersIndexRunner(BaseRunner[List[Dict[str, Any]]]):
    """
    Lists a saved scenario's users, each with their email, role and whether they are pending.

    GET /api/v4/saved_scenarios/{id}/users
    """

    @staticmethod
    def run(client: BaseClient, saved_scenario_id: int, **kwargs: Any) -> ServiceResult[List[Dict[str, Any]]]:
        return SavedScenarioUsersIndexRunner._make_myetm_request(
            client, "get", f"/saved_scenarios/{saved_scenario_id}/users", **kwargs
        )
