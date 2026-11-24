"""Runner for listing the caller's collections in MyETM."""

from typing import Any, Dict, List

from pyetm.clients.base_client import BaseClient
from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult


class FetchUserCollectionsRunner(BaseRunner[List[Dict[str, Any]]]):
    """
    Lists every collection the caller owns, across all pages.

    GET /api/v4/collections
    """

    @staticmethod
    def run(client: BaseClient, **kwargs: Any) -> ServiceResult[List[Dict[str, Any]]]:
        return FetchUserCollectionsRunner._fetch_all_pages(client, "/collections")
