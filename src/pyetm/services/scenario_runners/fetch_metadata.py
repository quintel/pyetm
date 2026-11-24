"""Service for fetch metadata operations."""

from typing import Any, Dict

from pyetm.services.scenario_runners.base_runner import BaseRunner, scenario_path
from ..service_result import ServiceResult
from pyetm.clients.base_client import BaseClient


class FetchMetadataRunner(BaseRunner[Dict[str, Any]]):
    """
    Reads the current Session's own fields, such as start_year, keep_compatible and template.

    GET /api/v4/saved_scenarios/{id}/
    """

    @staticmethod
    def run(
        client: BaseClient, scenario: Any, **kwargs: Any
    ) -> ServiceResult[Dict[str, Any]]:
        return FetchMetadataRunner._make_request(client, "get", scenario_path(scenario))
