import io
from unittest.mock import Mock, patch
from pyetm.services.scenario_runners.fetch_curves_generic import (
    GenericCurveDownloadRunner,
    GenericCurveBulkRunner,
)
from pyetm.clients.base_client import BaseClient
from pyetm.clients.session import ETMResponse
from pyetm.services.service_result import ServiceResult


def _ok_csv_response(text: str) -> ETMResponse:
    return ETMResponse(
        status_code=200,
        headers={"content-type": "text/csv"},
        url="/x",
        text=text,
        _content=text.encode("utf-8"),
    )


def _client_scenario(scenario_id: int = 123):
    mock_client = Mock(spec=BaseClient)
    mock_client.session = Mock()
    scenario = Mock()
    scenario.id = scenario_id
    return mock_client, scenario


def test_generic_curve_download_runner_success():
    mock_client, mock_scenario = _client_scenario()

    with patch.object(
        GenericCurveDownloadRunner,
        "_make_batch_requests",
        return_value=[ServiceResult.ok(data=_ok_csv_response("time,value\n1,2\n3,4"))],
    ):
        result = GenericCurveDownloadRunner.run(mock_client, mock_scenario, "test_curve")
    assert result.success and result.data.getvalue() == "time,value\n1,2\n3,4"


def test_generic_curve_download_runner_custom_type():
    mock_client, mock_scenario = _client_scenario(456)

    with patch.object(
        GenericCurveDownloadRunner,
        "_make_batch_requests",
        return_value=[ServiceResult.ok(data=_ok_csv_response("csv,data"))],
    ) as patched:
        result = GenericCurveDownloadRunner.run(
            mock_client, mock_scenario, "custom_curve", curve_type="custom"
        )
    assert result.success
    called_req = patched.call_args[0][1][0]
    assert "custom_curves/custom_curve.csv" in called_req["path"]


def test_renamed_curve_uses_old_wire_name():
    """A renamed curve is requested under its universal old name (works on pro + 2025-01)."""
    mock_client, mock_scenario = _client_scenario()

    with patch.object(
        GenericCurveDownloadRunner,
        "_make_batch_requests",
        return_value=[ServiceResult.ok(data=_ok_csv_response("a,b"))],
    ) as patched:
        GenericCurveDownloadRunner.run(mock_client, mock_scenario, "district_heating_profiles")
    assert patched.call_args[0][1][0]["path"] == "/scenarios/123/curves/heat_network.csv"


def test_generic_curve_download_runner_http_error():
    mock_client, mock_scenario = _client_scenario()

    with patch.object(
        GenericCurveDownloadRunner,
        "_make_batch_requests",
        return_value=[ServiceResult.fail(["404: Not Found"])],
    ):
        result = GenericCurveDownloadRunner.run(mock_client, mock_scenario, "missing_curve")
    assert not result.success and "404: Not Found" in result.errors


def test_generic_curve_download_runner_exception():
    mock_client, mock_scenario = _client_scenario()
    with patch.object(
        GenericCurveDownloadRunner,
        "_make_batch_requests",
        side_effect=RuntimeError("boom"),
    ):
        result = GenericCurveDownloadRunner.run(mock_client, mock_scenario, "test_curve")
    assert not result.success and "boom" in result.errors[0]


def test_generic_curve_bulk_runner_success():
    mock_client, mock_scenario = _client_scenario()

    ok_resp = _ok_csv_response("a,b\n1,2")
    with patch.object(
        GenericCurveBulkRunner,
        "_make_batch_requests",
        return_value=[ServiceResult.ok(data=ok_resp), ServiceResult.ok(data=ok_resp)],
    ):
        result = GenericCurveBulkRunner.run(
            mock_client, mock_scenario, ["curve1", "curve2"], batch_size=10
        )
    assert result.success and set(result.data.keys()) == {"curve1", "curve2"}


def test_generic_curve_bulk_runner_keys_by_canonical():
    """Old names passed in are keyed by canonical name in the result."""
    mock_client, mock_scenario = _client_scenario()

    ok_resp = _ok_csv_response("a,b")
    with patch.object(
        GenericCurveBulkRunner,
        "_make_batch_requests",
        return_value=[ServiceResult.ok(data=ok_resp)],
    ) as patched:
        result = GenericCurveBulkRunner.run(
            mock_client, mock_scenario, ["merit_order"], batch_size=10
        )
    # keyed by canonical, requested under old wire name
    assert set(result.data.keys()) == {"electricity_profiles"}
    assert patched.call_args[0][1][0]["path"] == "/scenarios/123/curves/merit_order.csv"


def test_generic_curve_bulk_runner_partial_failure():
    mock_client, mock_scenario = _client_scenario()

    ok_resp = _ok_csv_response("a,b")
    with patch.object(
        GenericCurveBulkRunner,
        "_make_batch_requests",
        return_value=[
            ServiceResult.ok(data=ok_resp),
            ServiceResult.fail(["404: Not Found"]),
        ],
    ):
        result = GenericCurveBulkRunner.run(
            mock_client, mock_scenario, ["curve1", "curve2"], batch_size=10
        )
    assert result.success and "curve1" in result.data and any("curve2" in e for e in result.errors)


def test_generic_curve_bulk_runner_all_fail():
    mock_client, mock_scenario = _client_scenario()

    with patch.object(
        GenericCurveBulkRunner,
        "_make_batch_requests",
        return_value=[
            ServiceResult.fail(["500: Server Error"]),
            ServiceResult.fail(["500: Server Error"]),
        ],
    ):
        result = GenericCurveBulkRunner.run(
            mock_client, mock_scenario, ["curve1", "curve2"], batch_size=10
        )
    assert not result.success and len(result.errors) >= 2


def test_generic_curve_bulk_runner_unexpected_exception():
    mock_client, mock_scenario = _client_scenario()
    with patch.object(
        GenericCurveBulkRunner,
        "_make_batch_requests",
        side_effect=RuntimeError("Unexpected error"),
    ):
        result = GenericCurveBulkRunner.run(mock_client, mock_scenario, ["curve1"], batch_size=10)
    assert not result.success and "Unexpected error" in result.errors[0]
