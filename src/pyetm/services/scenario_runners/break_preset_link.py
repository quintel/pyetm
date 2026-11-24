"""Runner for breaking a scenario's preset link."""

from typing import Any, Dict

from pyetm.clients.base_client import BaseClient
from pyetm.services.scenario_runners.base_runner import BaseRunner, scenario_path
from ..service_result import ServiceResult


class BreakPresetLinkRunner(BaseRunner[Dict[str, Any]]):
    """
    Clears the preset link of a scenario's Session, making it independent of the scenario it was
    copied from.

    PUT /api/v4/saved_scenarios/{id}/
    """

    @staticmethod
    def run(client: BaseClient, scenario: Any, **kwargs: Any) -> ServiceResult[Dict[str, Any]]:
        return BreakPresetLinkRunner._make_request(
            client, "put", scenario_path(scenario), payload={"scenario": {"preset_scenario_id": None}}
        )
