import io
from typing import Any, Dict, Literal, Optional, TypeVar, Generic
from abc import ABC, abstractmethod
from ..service_result import ServiceResult
from pyetm.clients.async_base_client import AsyncBaseClient

T = TypeVar("T")


class AsyncBaseRunner(ABC, Generic[T]):
    """
    Base class for all async API runners that handles common HTTP request patterns
    and error handling for both read and write operations.
    """

    @classmethod
    async def _make_request(
        cls,
        client: AsyncBaseClient,
        method: str,
        path: str,
        payload: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> ServiceResult[Any]:
        """
        Make an async HTTP request and handle common error patterns.

        Args:
            client: The async HTTP client to use
            method: HTTP method (get, post, put, patch, delete)
            path: API endpoint path
            payload: Data to send in request body (for write operations)
            **kwargs: Additional arguments passed to the request

        Returns:
            ServiceResult.ok(data) on success (2xx responses)
            ServiceResult.fail(errors) on any error
        """
        try:
            # Prepare request arguments
            request_kwargs = dict(kwargs)

            # Handle payload based on HTTP method
            if payload is not None:
                if method.upper() in ["POST", "PUT", "PATCH"]:
                    request_kwargs["json"] = payload
                else:
                    # For GET/DELETE, treat payload as query parameters
                    request_kwargs["params"] = payload

            # Make the async request
            resp = await client.session.request(method.upper(), path, **request_kwargs)

            if resp.status_code < 400:
                # For JSON responses, parse automatically
                try:
                    return ServiceResult.ok(data=resp.json())
                except ValueError:
                    # Not JSON, return raw response
                    return ServiceResult.ok(data=resp)

            # HTTP-level failure is breaking
            return ServiceResult.fail([f"{resp.status_code}: {resp.text}"])

        except (PermissionError, ValueError, ConnectionError) as e:
            # These are HTTP errors from our _handle_errors method
            return ServiceResult.fail([str(e)])
        except Exception as e:
            # Any other unexpected exception is treated as breaking
            return ServiceResult.fail([str(e)])

    @staticmethod
    @abstractmethod
    async def run(client: AsyncBaseClient, scenario: Any, **kwargs) -> ServiceResult[T]:
        """Subclasses must implement this async method."""
        pass


# Example of how your existing runners would be converted:


class AsyncUpdateCustomCurvesRunner(AsyncBaseRunner[Dict[str, Any]]):
    """
    Async runner for uploading custom curves to a scenario.
    """

    @staticmethod
    async def run(
        client: AsyncBaseClient,
        scenario: Any,
        custom_curves: Any,
        **kwargs,
    ) -> ServiceResult[Dict[str, Any]]:
        """Upload all curves in the CustomCurves object concurrently."""

        import asyncio

        async def upload_single_curve(curve):
            """Helper function to upload a single curve."""
            try:
                if curve.file_path and curve.file_path.exists():
                    # Use file
                    with open(curve.file_path, "r") as f:
                        files = {
                            "file": (f"{curve.key}.csv", f, "application/octet-stream")
                        }
                        # Override Content-Type header so multipart/form-data is used
                        headers = {"Content-Type": None}

                        result = await AsyncUpdateCustomCurvesRunner._make_request(
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

                    result = await AsyncUpdateCustomCurvesRunner._make_request(
                        client=client,
                        method="put",
                        path=f"/scenarios/{scenario.id}/custom_curves/{curve.key}",
                        files=files,
                        headers=headers,
                    )

                return curve.key, result

            except Exception as e:
                return curve.key, ServiceResult.fail(
                    [f"Error uploading {curve.key}: {str(e)}"]
                )

        # Run all uploads concurrently
        tasks = [upload_single_curve(curve) for curve in custom_curves.curves]
        results = await asyncio.gather(*tasks)

        # Process results
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


class AsyncGenericCurveDownloadRunner(AsyncBaseRunner[io.StringIO]):
    """
    Async generic runner for downloading any curve as CSV data.
    """

    @staticmethod
    async def run(
        client: AsyncBaseClient,
        scenario: Any,
        curve_name: str,
        curve_type: Literal["custom", "output"] = "output",
    ) -> ServiceResult[io.StringIO]:
        """
        Download a curve as CSV data asynchronously.
        """
        # Build URL path
        if curve_type == "custom":
            url_path = f"/scenarios/{scenario.id}/custom_curves/{curve_name}.csv"
        else:
            url_path = f"/scenarios/{scenario.id}/curves/{curve_name}.csv"

        try:
            resp = await client.session.request("GET", url_path)

            if resp.status_code < 400:
                csv_data = io.StringIO(resp.content.decode("utf-8"))
                return ServiceResult.ok(data=csv_data)

            return ServiceResult.fail([f"{resp.status_code}: {resp.text}"])

        except (PermissionError, ValueError, ConnectionError) as e:
            return ServiceResult.fail([str(e)])
        except Exception as e:
            return ServiceResult.fail([str(e)])


class AsyncGenericCurveBulkRunner(AsyncBaseRunner[Dict[str, io.StringIO]]):
    """
    Async generic runner for downloading multiple curves concurrently.
    """

    @staticmethod
    async def run(
        client: AsyncBaseClient,
        scenario: Any,
        curve_names: list[str],
        curve_type: Literal["custom", "output"] = "output",
    ) -> ServiceResult[Dict[str, io.StringIO]]:
        """
        Download multiple curves concurrently.
        """
        import asyncio

        async def download_single_curve(curve_name: str):
            """Helper to download a single curve."""
            try:
                result = await AsyncGenericCurveDownloadRunner.run(
                    client, scenario, curve_name, curve_type
                )
                return curve_name, result
            except Exception as e:
                return curve_name, ServiceResult.fail([f"Unexpected error - {str(e)}"])

        # Download all curves concurrently
        tasks = [download_single_curve(curve_name) for curve_name in curve_names]
        results = await asyncio.gather(*tasks)

        # Process results
        curves_data = {}
        errors = []

        for curve_name, result in results:
            if result.success:
                curves_data[curve_name] = result.data
            else:
                errors.extend([f"{curve_name}: {error}" for error in result.errors])

        if curves_data:
            return ServiceResult.ok(data=curves_data, errors=errors if errors else None)
        else:
            return ServiceResult.fail(
                errors if errors else ["No curves could be downloaded"]
            )
