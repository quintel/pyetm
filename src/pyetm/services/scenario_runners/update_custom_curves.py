from typing import Any, Dict
from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult
from pyetm.clients.base_client import BaseClient


class UpdateCustomCurvesRunner(BaseRunner[Dict[str, Any]]):
    """Runner for uploading custom curves to a scenario."""

    @staticmethod
    def run(
        client: BaseClient, scenario: Any, custom_curves: Any, **kwargs
    ) -> ServiceResult[Dict[str, Any]]:

        requests = []
        curve_keys = []

        for curve in custom_curves.curves:
            if curve.file_path and curve.file_path.exists():
                with open(curve.file_path, "r") as f:
                    content = f.read()
            else:
                curve_data = curve.contents()
                content = "\n".join(str(value) for value in curve_data)

            requests.append(
                {
                    "method": "put",
                    "path": f"/scenarios/{scenario.id}/custom_curves/{curve.key}",
                    "payload": None,
                    "kwargs": {
                        "files": {
                            "file": (
                                f"{curve.key}.csv",
                                content,
                                "application/octet-stream",
                            )
                        },
                        "headers": {"Content-Type": None},
                    },
                }
            )
            curve_keys.append(curve.key)

        # Execute all uploads concurrently
        results = UpdateCustomCurvesRunner._make_batch_requests(client, requests)
        successful_uploads = []
        all_errors = []

        for curve_key, result in zip(curve_keys, results):
            if result.success:
                successful_uploads.append(curve_key)
            else:
                for err in result.errors:
                    all_errors.append(f"{curve_key}: {err}")

        return ServiceResult(
            success=len(all_errors) == 0,
            data={
                "uploaded_curves": successful_uploads,
                "total_curves": len(custom_curves.curves),
                "successful_uploads": len(successful_uploads),
            },
            errors=all_errors,
        )
