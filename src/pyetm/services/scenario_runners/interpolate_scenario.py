from typing import Any, Dict, Optional
from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult
from pyetm.clients.base_client import BaseClient


class InterpolateScenarioRunner(BaseRunner[Dict[str, Any]]):
    """
    Runner for interpolating a scenario to a different end year, optionally
    using a start scenario as a baseline.

    POST /api/v3/scenarios/:id/interpolate

    Args:
        client: The HTTP client to use
        scenario_id: ID of the end scenario to interpolate from
        end_year: Target year for the interpolated scenario
        start_scenario_id: Optional ID of a start scenario to use as baseline
        **kwargs: Additional arguments passed to the request
    """

    @staticmethod
    def run(
        client: BaseClient,
        scenario_id: int,
        end_year: int,
        start_scenario_id: Optional[int] = None,
        **kwargs,
    ) -> ServiceResult[Dict[str, Any]]:
        """
        Example usage:
            # Interpolate single scenario from 2050 to 2040
            result = InterpolateScenarioRunner.run(
                client=client,
                scenario_id=123456,
                end_year=2040
            )

            # Interpolate between two scenarios (2030 -> 2050) to create 2040
            result = InterpolateScenarioRunner.run(
                client=client,
                scenario_id=end_scenario_id,  # 2050 scenario
                end_year=2040,
                start_scenario_id=start_scenario_id  # 2030 scenario
            )

        Returns:
            ServiceResult with the new interpolated scenario data on success,
            or error messages on failure (404, 422, etc.)
        """
        payload = {"end_year": end_year}

        if start_scenario_id is not None:
            payload["start_scenario_id"] = start_scenario_id

        result = InterpolateScenarioRunner._make_request(
            client=client,
            method="post",
            path=f"/scenarios/{scenario_id}/interpolate",
            payload=payload,
        )

        return result
