from typing import Any, Dict
from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult
from pyetm.clients.base_client import BaseClient


class UpdateCustomCurvesRunner(BaseRunner[Dict[str, Any]]):
    """
    Runner for uploading custom curves to a scenario.
    """

    @staticmethod
    def run(
        client: BaseClient,
        scenario: Any,
        custom_curves: Any,
        **kwargs,
    ) -> ServiceResult[Dict[str, Any]]:
        """Upload all curves in the CustomCurves object."""

        all_errors = []
        successful_uploads = []

        for curve in custom_curves.curves:
            try:
                if curve.file_path and curve.file_path.exists():
                    # Use file
                    with open(curve.file_path, "r") as f:
                        files = {
                            "file": (f"{curve.key}.csv", f, "application/octet-stream")
                        }
                        # Override Content-Type header so multipart/form-data is used
                        headers = {"Content-Type": None}

                        result = UpdateCustomCurvesRunner._make_request(
                            client=client,
                            method="put",
                            path=f"/scenarios/{scenario.id}/custom_curves/{curve.key}",
                            files=files,
                            headers=headers,
                        )
                else:
                    # Create file content from curve data
                    curve_data = curve.contents()
                    file_content = "\n".join(str(value) for value in curve_data)
                    files = {
                        "file": (
                            f"{curve.key}.csv",
                            file_content,
                            "application/octet-stream",
                        )
                    }
                    # Override Content-Type header so multipart/form-data is used
                    headers = {"Content-Type": None}

                    result = UpdateCustomCurvesRunner._make_request(
                        client=client,
                        method="put",
                        path=f"/scenarios/{scenario.id}/custom_curves/{curve.key}",
                        files=files,
                        headers=headers,
                    )

                # Check if the request was successful
                if result.success:
                    successful_uploads.append(curve.key)
                else:
                    for err in result.errors:
                        all_errors.append(f"{curve.key}: {err}")

            except Exception as e:
                all_errors.append(f"Error uploading {curve.key}: {str(e)}")

        # TODO: This provides some aggregated results, because we actually get multiple ServiceResults - one for each curve upload. Explore further.
        return ServiceResult(
            success=len(all_errors) == 0,
            data={
                "uploaded_curves": successful_uploads,
                "total_curves": len(custom_curves.curves),
                "successful_uploads": len(successful_uploads),
            },
            errors=all_errors,
        )
