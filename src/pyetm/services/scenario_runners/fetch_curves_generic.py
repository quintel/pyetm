"""Service for fetch curves generic operations."""

import io
from typing import Any, Dict, Literal, List, Optional
from pyetm.services.scenario_runners.base_runner import BaseRunner
from ..service_result import ServiceResult
from pyetm.clients.base_client import BaseClient
from pyetm.config import curve_registry as registry


def _session_id(scenario: Any) -> Any:
    from pyetm.models.scenario import Scenario
    return scenario.session.id if isinstance(scenario, Scenario) else scenario.id


def _output_path(session_id: Any, name: str) -> str:
    return registry.build_path(session_id, name)


def _custom_path(session_id: Any, name: str) -> str:
    return f"/scenarios/{session_id}/custom_curves/{name}.csv"


def _request(path: str) -> Dict[str, Any]:
    return {
        "method": "get",
        "path": path,
        "payload": None,
        "kwargs": {"headers": {"Accept": "text/csv"}},
    }


class GenericCurveDownloadRunner(BaseRunner[Any]):
    """
    Generic runner for downloading any curve as CSV data.
    Supports both custom curves and output curves.
    """

    @staticmethod
    def run(
        client: BaseClient,
        scenario: Any,
        curve_name: str,
        curve_type: Literal["custom", "output"] = "output",
        **kwargs: Any,
    ) -> ServiceResult[Any]:
        """Execute the curve download operation.

        Returns:
            ServiceResult[io.StringIO]: Success case contains StringIO with CSV data;
                failure case contains error messages.
        """
        session_id = _session_id(scenario)
        path = (
            _custom_path(session_id, curve_name)
            if curve_type == "custom"
            else _output_path(session_id, curve_name)
        )

        try:
            result = GenericCurveDownloadRunner._make_batch_requests(client, [_request(path)])[0]
        except Exception as e:
            return ServiceResult.fail([str(e)])

        if not result.success:
            return ServiceResult.fail(result.errors)
        try:
            resp = result.data
            return ServiceResult.ok(data=io.StringIO(resp.content.decode("utf-8")))  # type: ignore[union-attr]
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
        session_id: Any, curve_names: List[str], curve_type: Literal["custom", "output"]
    ) -> List[Dict[str, Any]]:
        """Build (canonical_name, request) per curve. Results are keyed by canonical name so
        cache files and curve keys stay independent of the on-the-wire (old) name."""
        requests: List[Dict[str, Any]] = []
        for name in curve_names:
            if curve_type == "custom":
                requests.append({"name": name, **_request(_custom_path(session_id, name))})
            else:
                canonical = registry.accept_alias(name)
                requests.append(
                    {"name": canonical, **_request(_output_path(session_id, canonical))}
                )
        return requests

    @staticmethod
    def run(
        client: BaseClient,
        scenario: Any,
        curve_names: List[str],
        curve_type: Literal["custom", "output"] = "output",
        batch_size: Optional[int] = None,
        **kwargs: Any,
    ) -> ServiceResult[Dict[str, Any]]:
        batch_size = batch_size or (
            GenericCurveBulkRunner.DEFAULT_BATCH_SIZE_OUTPUT
            if curve_type == "output"
            else GenericCurveBulkRunner.DEFAULT_BATCH_SIZE_CUSTOM
        )

        session_id = _session_id(scenario)
        all_requests = GenericCurveBulkRunner._build_requests(session_id, curve_names, curve_type)

        curves_data: Dict[str, io.StringIO] = {}
        errors: List[str] = []

        for start in range(0, len(all_requests), batch_size):
            chunk = all_requests[start : start + batch_size]
            try:
                results = GenericCurveBulkRunner._make_batch_requests(client, chunk)
            except Exception as e:
                for req in chunk:
                    errors.append(f"{req['name']}: batch error {e}")
                continue

            for req, result in zip(chunk, results):
                name = req["name"]
                if result.success:
                    try:
                        resp = result.data
                        curves_data[name] = io.StringIO(resp.content.decode("utf-8"))  # type: ignore[union-attr]
                    except Exception as e:
                        errors.append(f"{name}: parse error {e}")
                else:
                    for err in result.errors:
                        errors.append(f"{name}: {err}")

        if curves_data:
            return ServiceResult.ok(data=curves_data, errors=errors if errors else None)
        return ServiceResult.fail(errors or ["No curves could be downloaded"])
