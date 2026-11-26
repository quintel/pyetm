"""Runner for listing the caller's saved scenarios in MyETM."""

from typing import Any, Dict, List

from pyetm.clients.base_client import BaseClient
from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult


class FetchUserSavedScenariosRunner(BaseRunner[List[Dict[str, Any]]]):
    """
    Lists every saved scenario the caller can access, across all pages.

    GET /api/v4/saved_scenarios
    """

    @staticmethod
    def run(client: BaseClient, **kwargs: Any) -> ServiceResult[List[Dict[str, Any]]]:
        return FetchUserSavedScenariosRunner._fetch_all_pages(client, "/saved_scenarios")
