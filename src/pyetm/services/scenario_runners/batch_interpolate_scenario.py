from typing import Any, Dict, List
from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult
from pyetm.clients.base_client import BaseClient


class BatchInterpolateScenarioRunner(BaseRunner[List[Dict[str, Any]]]):
    """
    Runner for batch interpolating multiple scenarios to create intermediate year scenarios.

    POST /api/v3/scenarios/interpolate
    """

    @staticmethod
    def run(
        client: BaseClient,
        scenario_ids: List[int],
        end_years: List[int],
        **kwargs,
    ) -> ServiceResult[List[Dict[str, Any]]]:
        payload = {
            "scenario_ids": scenario_ids,
            "end_years": end_years,
        }

        result = BatchInterpolateScenarioRunner._make_request(
            client=client,
            method="post",
            path="/scenarios/interpolate",
            payload=payload,
        )

        return result
