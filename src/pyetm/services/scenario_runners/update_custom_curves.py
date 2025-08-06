import requests
from typing import Any, Dict
from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult
from pyetm.clients.base_client import BaseClient


class UpdateCustomCurvesRunner(BaseRunner[Dict[str, Any]]):
    """
    Runner for uploading custom curves to a scenario.
    Uses raw requests to match the successful manual upload approach.
    """

    @staticmethod
    def run(
        client: BaseClient,
        scenario: Any,
        custom_curves: Any,  # CustomCurves object
        **kwargs,
    ) -> ServiceResult[Dict[str, Any]]:
        """Upload all curves in the CustomCurves object."""

        all_errors = []
        successful_uploads = []

        auth_header = client.session.headers.get("Authorization")
        base_url = str(client.session.base_url).rstrip("/")
        if base_url.endswith("/api/v3"):
            base_url = base_url[:-7]

        for curve in custom_curves.curves:
            try:
                # Upload the curve using raw requests (required for file uploads)
                url = f"{base_url}/api/v3/scenarios/{scenario.id}/custom_curves/{curve.key}"
                headers = {"Authorization": auth_header}

                if curve.file_path and curve.file_path.exists():
                    # Use actual file
                    with open(curve.file_path, "rb") as f:
                        files = {
                            "file": (f"{curve.key}.csv", f, "application/octet-stream")
                        }
                        response = requests.put(url, files=files, headers=headers)
                else:
                    # Create file content from curve data
                    curve_data = curve.contents()
                    file_content = "\n".join(str(value) for value in curve_data.values)
                    files = {
                        "file": (
                            f"{curve.key}.csv",
                            file_content,
                            "application/octet-stream",
                        )
                    }
                    response = requests.put(url, files=files, headers=headers)

                # Check response
                if response.status_code in [200, 201, 204]:
                    successful_uploads.append(curve.key)
                else:
                    error_msg = (
                        f"Failed to upload {curve.key}: HTTP {response.status_code}"
                    )
                    try:
                        error_data = response.json()
                        if "errors" in error_data:
                            error_msg += f" - {error_data['errors']}"
                    except:
                        if response.text:
                            error_msg += f" - {response.text}"
                    all_errors.append(error_msg)

            except Exception as e:
                all_errors.append(f"Error uploading {curve.key}: {str(e)}")

        return ServiceResult(
            success=len(all_errors) == 0,
            data={
                "uploaded_curves": successful_uploads,
                "total_curves": len(custom_curves.curves),
                "successful_uploads": len(successful_uploads),
            },
            errors=all_errors,
        )
