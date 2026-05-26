"""Service for fetch annual exports operations."""

import io
from typing import Any
from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult
from pyetm.clients.base_client import BaseClient


class DownloadAnnualExportRunner(BaseRunner[Any]):
    """
    Download a specific annual export as CSV data.
    """

    @staticmethod
    def run(
        client: BaseClient, scenario: Any, export_name: str, **kwargs: Any
    ) -> ServiceResult[Any]:
        """
        Download a single annual export.
        """
        path = f"/scenarios/{scenario.id}/{export_name}"

        req = [
            {
                "method": "get",
                "path": path,
                "payload": None,
                "kwargs": {"headers": {"Accept": "text/csv"}},
            }
        ]

        try:
            result = DownloadAnnualExportRunner._make_batch_requests(client, req)[0]
        except Exception as e:
            return ServiceResult.fail([f"Request failed for {export_name}: {str(e)}"])

        if not result.success:
            return ServiceResult.fail(result.errors)

        try:
            resp = result.data
            return ServiceResult.ok(data=io.StringIO(resp.content.decode("utf-8")))  # type: ignore[union-attr]
        except Exception as e:
            return ServiceResult.fail([f"Failed to parse export data for {export_name}: {e}"])


def download_annual_export(
    client: BaseClient, scenario: Any, export_name: str
) -> ServiceResult[Any]:
    """
    Convenience function to download a single annual export.
    """
    return DownloadAnnualExportRunner.run(client, scenario, export_name)
