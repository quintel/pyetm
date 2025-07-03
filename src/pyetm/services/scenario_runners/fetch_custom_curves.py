import io
from typing import Any, Dict
from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult
from pyetm.clients.base_client import BaseClient


class DownloadCurveRunner(BaseRunner[io.StringIO]):
    """
    Runner for downloading a specific custom curve as CSV data.
    GET /api/v3/scenarios/{scenario_id}/custom_curves/{curve_name}.csv

    Returns:
        ServiceResult.ok(data) where `data` is a StringIO object containing the CSV data.
        ServiceResult.fail(errors) on any breaking error.
    """

    @staticmethod
    def run(
        client: BaseClient,
        scenario: Any,
        curve_name: str,
    ) -> ServiceResult[io.StringIO]:
        # Make the request manually to get raw response (not auto-parsed JSON)
        try:
            resp = client.session.get(
                f"/scenarios/{scenario.id}/custom_curves/{curve_name}.csv"
            )

            if resp.ok:
                # Convert response to StringIO
                csv_data = io.StringIO(resp.content.decode("utf-8"))
                return ServiceResult.ok(data=csv_data)

            # HTTP-level failure is breaking
            return ServiceResult.fail([f"{resp.status_code}: {resp.text}"])

        except (PermissionError, ValueError, ConnectionError) as e:
            # These are HTTP errors from our _handle_errors method
            return ServiceResult.fail([str(e)])
        except Exception as e:
            # Any other unexpected exception is treated as breaking
            return ServiceResult.fail([str(e)])


class FetchAllCurveDataRunner(BaseRunner[Dict[str, Any]]):
    """
    Runner for fetching metadata for all custom curves on a scenario.
    GET /api/v3/scenarios/{scenario_id}/custom_curves

    Returns:
        ServiceResult.ok(data) where `data` is a dict containing curve metadata.
        ServiceResult.fail(errors) on any breaking error.
    """

    @staticmethod
    def run(
        client: BaseClient,
        scenario: Any,
    ) -> ServiceResult[Dict[str, Any]]:
        return FetchAllCurveDataRunner._make_request(
            client=client, method="get", path=f"/scenarios/{scenario.id}/custom_curves"
        )
