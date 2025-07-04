import io
from typing import Any, Dict, Literal
from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult
from pyetm.clients.base_client import BaseClient


class GenericCurveDownloadRunner(BaseRunner[io.StringIO]):
    """
    Generic runner for downloading any curve as CSV data.
    Supports both custom curves and output curves.

    Returns:
        ServiceResult.ok(data) where `data` is a StringIO object containing the CSV data.
        ServiceResult.fail(errors) on any breaking error.
    """

    @staticmethod
    def run(
        client: BaseClient,
        scenario: Any,
        curve_name: str,
        curve_type: Literal["custom", "output"] = "output",
    ) -> ServiceResult[io.StringIO]:
        """
        Download a curve as CSV data.

        Args:
            client: API client
            scenario: Scenario object
            curve_name: Name of the curve to download
            curve_type: "custom" for custom_curves endpoint, "output" for curves endpoint
        """
        # Build URL path
        if curve_type == "custom":
            url_path = f"/scenarios/{scenario.id}/custom_curves/{curve_name}.csv"
        else:
            url_path = f"/scenarios/{scenario.id}/curves/{curve_name}.csv"

        try:
            resp = client.session.get(url_path)

            if resp.ok:
                csv_data = io.StringIO(resp.content.decode("utf-8"))
                return ServiceResult.ok(data=csv_data)

            return ServiceResult.fail([f"{resp.status_code}: {resp.text}"])

        except (PermissionError, ValueError, ConnectionError) as e:
            return ServiceResult.fail([str(e)])
        except Exception as e:
            return ServiceResult.fail([str(e)])


class GenericCurveBulkRunner(BaseRunner[Dict[str, io.StringIO]]):
    """
    Generic runner for downloading multiple curves at once.
    """

    @staticmethod
    def run(
        client: BaseClient,
        scenario: Any,
        curve_names: list[str],
        curve_type: Literal["custom", "output"] = "output",
    ) -> ServiceResult[Dict[str, io.StringIO]]:
        """
        Download multiple curves.

        Args:
            client: API client
            scenario: Scenario object
            curve_names: List of curve names to download
            curve_type: "custom" or "output"
        """
        curves_data = {}
        errors = []

        for curve_name in curve_names:
            try:
                result = GenericCurveDownloadRunner.run(
                    client, scenario, curve_name, curve_type
                )

                if result.success:
                    curves_data[curve_name] = result.data
                else:
                    errors.extend([f"{curve_name}: {error}" for error in result.errors])

            except Exception as e:
                errors.append(f"{curve_name}: Unexpected error - {str(e)}")

        if curves_data:
            return ServiceResult.ok(data=curves_data, errors=errors if errors else None)
        else:
            return ServiceResult.fail(
                errors if errors else ["No curves could be downloaded"]
            )
