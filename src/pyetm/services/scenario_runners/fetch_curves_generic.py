import io
from typing import Any, Dict, Literal, List, Optional
from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult
from pyetm.clients.base_client import BaseClient


class GenericCurveDownloadRunner(BaseRunner[Any]):
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
    ) -> ServiceResult[Any]:
        path = (
            f"/scenarios/{scenario.id}/custom_curves/{curve_name}.csv"
            if curve_type == "custom"
            else f"/scenarios/{scenario.id}/curves/{curve_name}.csv"
        )
        req = [
            {
                "method": "get",
                "path": path,
                "payload": None,
                "kwargs": {"headers": {"Accept": "text/csv"}},
            }
        ]
        try:
            result = GenericCurveDownloadRunner._make_batch_requests(client, req)[0]
        except Exception as e:
            return ServiceResult.fail([str(e)])

        if not result.success:
            return ServiceResult.fail(result.errors)
        try:
            resp = result.data
            # TODO: is this ok to return a io object??
            # Is this what causes IO pressure?
            return ServiceResult.ok(data=io.StringIO(resp.content.decode("utf-8")))
        except Exception as e:
            return ServiceResult.fail([f"Failed to parse curve data: {e}"])


class GenericCurveBulkRunner(BaseRunner[Dict[str, io.StringIO]]):
    """
    Large output curves can cause high memory + IO pressure if fetched all at once; a
    batch size limit prevents overwhelming the event loop while still benefiting from
    concurrency.
    """

    # Large output curves can cause high memory + IO pressure - batch size limit avoids this
    DEFAULT_BATCH_SIZE_OUTPUT = 5
    DEFAULT_BATCH_SIZE_CUSTOM = 45

    @staticmethod
    def _build_requests(
        scenario: Any, curve_names: List[str], curve_type: Literal["custom", "output"]
    ) -> List[dict]:
        requests: List[dict] = []
        for name in curve_names:
            path = (
                f"/scenarios/{scenario.id}/custom_curves/{name}.csv"
                if curve_type == "custom"
                else f"/scenarios/{scenario.id}/curves/{name}.csv"
            )
            requests.append(
                {
                    "method": "get",
                    "path": path,
                    "payload": None,
                    "kwargs": {"headers": {"Accept": "text/csv"}},
                }
            )
        return requests

    # TODO: convert into generators that can return each item when its done
    # this will save more memory in the end!
    @staticmethod
    def run(
        client: BaseClient,
        scenario: Any,
        curve_names: List[str],
        curve_type: Literal["custom", "output"] = "output",
        batch_size: Optional[int] = None,
    ) -> ServiceResult[Dict[str, Any]]:
        batch_size = batch_size or (
            GenericCurveBulkRunner.DEFAULT_BATCH_SIZE_OUTPUT
            if curve_type == "output"
            else GenericCurveBulkRunner.DEFAULT_BATCH_SIZE_CUSTOM
        )

        curves_data: Dict[str, io.StringIO] = {}
        errors: List[str] = []

        all_requests = GenericCurveBulkRunner._build_requests(
            scenario, curve_names, curve_type
        )
        for start in range(0, len(all_requests), batch_size):
            chunk = all_requests[start : start + batch_size]
            try:
                results = GenericCurveBulkRunner._make_batch_requests(client, chunk)
            except Exception as e:
                for req in chunk:
                    name = req["path"].split("/")[-1].replace(".csv", "")
                    errors.append(f"{name}: batch error {e}")
                continue

            for req, result in zip(chunk, results):
                name = req["path"].split("/")[-1].replace(".csv", "")
                if result.success:
                    try:
                        resp = result.data
                        curves_data[name] = io.StringIO(resp.content.decode("utf-8"))
                    except Exception as e:
                        errors.append(f"{name}: parse error {e}")
                else:
                    for err in result.errors:
                        errors.append(f"{name}: {err}")

        if curves_data:
            return ServiceResult.ok(data=curves_data, errors=errors if errors else None)
        return ServiceResult.fail(errors or ["No curves could be downloaded"])
