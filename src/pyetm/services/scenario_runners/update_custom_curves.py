import asyncio
import io
from typing import Any, Dict
from pyetm.clients.async_base_client import AsyncBaseClient
from pyetm.services.scenario_runners.async_base_runner import AsyncBaseRunner
from ..service_result import ServiceResult


class UpdateCustomCurvesRunner(AsyncBaseRunner[Dict[str, Any]]):
    """
    Runner for uploading custom curves to a scenario concurrently.
    """

    @staticmethod
    async def run(
        client: AsyncBaseClient,
        scenario: Any,
        custom_curves: Any,
        **kwargs,
    ) -> ServiceResult[Dict[str, Any]]:
        """Upload all curves using requests in thread pool."""

        import asyncio
        import concurrent.futures
        import requests

        def upload_single_curve_sync(curve):
            """Sync function using requests - exactly like your working version."""
            try:
                # Create requests session
                session = requests.Session()
                session.headers.update(
                    {
                        "Authorization": f"Bearer {client.session.token}",
                        "Accept": "application/json",
                    }
                )

                base_url = str(client.session.base_url).rstrip("/")
                url = f"{base_url}/scenarios/{scenario.id}/custom_curves/{curve.key}"

                if curve.file_path and curve.file_path.exists():
                    # Use file - this is exactly how it worked before
                    with open(curve.file_path, "rb") as f:  # Note: binary mode
                        files = {
                            "file": (f"{curve.key}.csv", f, "application/octet-stream")
                        }
                        # This magic header setting works with requests
                        response = session.put(
                            url, files=files, headers={"Content-Type": None}
                        )
                else:
                    # Create file content from curve data
                    curve_data = curve.contents()
                    if curve_data is not None:
                        file_content = "\n".join(str(value) for value in curve_data)
                        files = {
                            "file": (
                                f"{curve.key}.csv",
                                file_content,
                                "application/octet-stream",
                            )
                        }
                        response = session.put(
                            url, files=files, headers={"Content-Type": None}
                        )
                    else:
                        return curve.key, ServiceResult.fail(
                            [f"No data available for curve {curve.key}"]
                        )

                if response.ok:
                    try:
                        data = response.json()
                    except:
                        data = {"status": "uploaded"}
                    return curve.key, ServiceResult.ok(data=data)
                else:
                    return curve.key, ServiceResult.fail(
                        [f"{response.status_code}: {response.text}"]
                    )

            except Exception as e:
                return curve.key, ServiceResult.fail(
                    [f"Error uploading {curve.key}: {str(e)}"]
                )
            finally:
                session.close()

        # Use thread pool for concurrency while keeping requests compatibility
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            loop = asyncio.get_event_loop()
            tasks = [
                loop.run_in_executor(executor, upload_single_curve_sync, curve)
                for curve in custom_curves.curves
            ]
            results = await asyncio.gather(*tasks)

        all_errors = []
        successful_uploads = []

        for curve_key, result in results:
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
