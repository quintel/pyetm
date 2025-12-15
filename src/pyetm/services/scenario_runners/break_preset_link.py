from typing import Any, Dict, Union
from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult
from pyetm.clients.base_client import BaseClient


class BreakPresetLinkRunner(BaseRunner[Dict[str, Any]]):
    """
    Runner for breaking the preset link on a scenario.

    This sets the preset_scenario_id to nil, making the scenario independent
    from its parent preset scenario.

    PUT /api/v3/scenarios/{scenario_id}

    Args:
        client: The HTTP client to use
        scenario: The scenario object (must have an 'id' attribute) or scenario ID
        **kwargs: Additional arguments passed to the request
    """

    @staticmethod
    def run(
        client: BaseClient,
        scenario: Union[Any, int],
        **kwargs
    ) -> ServiceResult[Dict[str, Any]]:
        """
        Break the preset link for a scenario.

        This makes the scenario independent by removing its connection to the
        preset scenario it was copied from.

        Example usage:
            result = BreakPresetLinkRunner.run(
                client=client,
                scenario=scenario_obj
            )

            # Or with scenario ID:
            result = BreakPresetLinkRunner.run(
                client=client,
                scenario=123456
            )
        """
        # Extract scenario ID
        scenario_id = scenario if isinstance(scenario, int) else scenario.id

        # Build payload to break the preset link
        payload = {"scenario": {"preset_scenario_id": None}}

        return BreakPresetLinkRunner._make_request(
            client=client,
            method="put",
            path=f"/scenarios/{scenario_id}",
            payload=payload,
        )
