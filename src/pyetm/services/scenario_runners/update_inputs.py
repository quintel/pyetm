"""Service for updating inputs on a session."""

from typing import Any, Dict
from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult
from pyetm.clients.base_client import BaseClient


class UpdateInputsRunner(BaseRunner[Dict[str, Any]]):
    """
    Runner for updating inputs on a scenario through the main scenario endpoint.

    PUT /api/v3/scenarios/{scenario_id}

    Args:
        client: The HTTP client to use
        scenario: The scenario object (must have an 'id' attribute)
        inputs: Dictionary of input updates to apply (input_key -> value)
        **kwargs: Additional arguments passed to the request
    """

    @staticmethod
    def build_request(scenario: Any, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """
        Build input update request for concurrent batching.

        Args:
            scenario: The scenario object (must have an 'id' attribute)
            inputs: Dictionary of input updates to apply

        Returns:
            Request dict ready for AsyncBatchRunner
        """
        return {
            "method": "put",
            "path": f"/scenarios/{scenario.id}",
            "payload": {"scenario": {"user_values": inputs}},
            "kwargs": {},
        }

    @staticmethod
    def run(
        client: BaseClient, scenario: Any, inputs: Dict[str, Any], **kwargs
    ) -> ServiceResult[Dict[str, Any]]:
        """
        Update inputs for a scenario.

        Example usage:
            result = UpdateInputsRunner.run(
                client=client,
                scenario=scenario,
                inputs={
                    "input_key_1": 42.5,
                    "input_key_2": 100.0,
                }
            )
        """
        payload = {"scenario": {"user_values": inputs}}

        return UpdateInputsRunner._make_request(
            client=client,
            method="put",
            path=f"/scenarios/{scenario.id}",
            payload=payload,
        )
