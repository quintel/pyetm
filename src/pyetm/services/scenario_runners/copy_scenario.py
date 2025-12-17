from typing import Any, Dict, Optional
from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult
from pyetm.clients.base_client import BaseClient


class CopyScenarioRunner(BaseRunner[Dict[str, Any]]):
    """
    Runner for copying an existing scenario.

    POST /api/v3/scenarios

    Args:
        client: The HTTP client to use
        scenario_id: ID of the scenario to copy
        overrides: Optional dictionary of scenario attributes to override
        **kwargs: Additional arguments passed to the request
    """

    # Fields that can be overridden when copying
    ALLOWED_OVERRIDE_KEYS = [
        "metadata",
        "source",
        "private",
        "keep_compatible",
        "set_preset_roles",
        "template",
    ]

    @staticmethod
    def run(
        client: BaseClient,
        scenario_id: int,
        overrides: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> ServiceResult[Dict[str, Any]]:
        """
        Copy an existing scenario with optional attribute overrides.

        Example usage:
            result = CopyScenarioRunner.run(
                client=client,
                scenario_id=123456,
                overrides={
                    "title": "Copy of my scenario",
                    "private": True,
                    "metadata": {"description": "A copy with new description"}
                }
            )
        """
        # Start with the scenario_id as base
        scenario_data = {"scenario_id": scenario_id}

        warnings = []

        # Merge in any overrides
        if overrides:
            filtered_overrides = {
                key: value
                for key, value in overrides.items()
                if key in CopyScenarioRunner.ALLOWED_OVERRIDE_KEYS
            }

            # Warn about ignored keys
            filtered_keys = set(overrides.keys()) - set(filtered_overrides.keys())
            for key in filtered_keys:
                warnings.append(f"Ignoring invalid field for scenario copy: {key!r}")

            scenario_data.update(filtered_overrides)

        # Transform template → preset_scenario_id for ETEngine API
        if "template" in scenario_data:
            scenario_data["preset_scenario_id"] = scenario_data.pop("template")

        payload = {"scenario": scenario_data}

        result = CopyScenarioRunner._make_request(
            client=client,
            method="post",
            path="/scenarios",
            payload=payload,
        )

        if result.success and warnings:
            # Merge our warnings with any from the API call
            combined_errors = list(result.errors) + warnings
            return ServiceResult.ok(data=result.data, errors=combined_errors)

        return result
