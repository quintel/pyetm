import io
from unittest.mock import Mock
from pyetm.services.scenario_runners.fetch_curves_generic import (
    GenericCurveDownloadRunner,
    GenericCurveBulkRunner,
)
from pyetm.clients.base_client import BaseClient


def test_generic_curve_download_runner_success():
    mock_client = Mock(spec=BaseClient)
    mock_client.session = Mock()
    mock_response = Mock()
    mock_response.ok = True
    mock_response.content = b"time,value\n1,2\n3,4"
    mock_client.session.get.return_value = mock_response

    mock_scenario = Mock()
    mock_scenario.id = 123

    result = GenericCurveDownloadRunner.run(mock_client, mock_scenario, "test_curve")

    assert result.success
    assert isinstance(result.data, io.StringIO)
    assert result.data.getvalue() == "time,value\n1,2\n3,4"


def test_generic_curve_download_runner_custom_type():
    mock_client = Mock(spec=BaseClient)
    mock_client.session = Mock()
    mock_response = Mock()
    mock_response.ok = True
    mock_response.content = b"csv,data"
    mock_client.session.get.return_value = mock_response

    mock_scenario = Mock()
    mock_scenario.id = 456

    result = GenericCurveDownloadRunner.run(
        mock_client, mock_scenario, "custom_curve", curve_type="custom"
    )

    assert result.success
    mock_client.session.get.assert_called_with(
        "/scenarios/456/custom_curves/custom_curve.csv"
    )


def test_generic_curve_download_runner_http_error():
    mock_client = Mock(spec=BaseClient)
    mock_client.session = Mock()
    mock_response = Mock()
    mock_response.ok = False
    mock_response.status_code = 404
    mock_response.text = "Not Found"
    mock_client.session.get.return_value = mock_response

    mock_scenario = Mock()
    mock_scenario.id = 123

    result = GenericCurveDownloadRunner.run(mock_client, mock_scenario, "missing_curve")

    assert not result.success
    assert "404: Not Found" in result.errors


def test_generic_curve_download_runner_exception():
    mock_client = Mock(spec=BaseClient)
    mock_client.session = Mock()
    mock_client.session.get.side_effect = ConnectionError("Network error")

    mock_scenario = Mock()
    mock_scenario.id = 123

    result = GenericCurveDownloadRunner.run(mock_client, mock_scenario, "test_curve")

    assert not result.success
    assert "Network error" in result.errors


def test_generic_curve_download_runner_unexpected_exception():
    mock_client = Mock(spec=BaseClient)
    mock_client.session = Mock()
    mock_client.session.get.side_effect = RuntimeError("Unexpected error")

    mock_scenario = Mock()
    mock_scenario.id = 123

    result = GenericCurveDownloadRunner.run(mock_client, mock_scenario, "test_curve")

    assert not result.success
    assert "Unexpected error" in result.errors


def test_generic_curve_bulk_runner_success():
    mock_client = Mock(spec=BaseClient)
    mock_client.session = Mock()
    mock_response = Mock()
    mock_response.ok = True
    mock_response.content = b"data"
    mock_client.session.get.return_value = mock_response

    mock_scenario = Mock()
    mock_scenario.id = 123

    result = GenericCurveBulkRunner.run(
        mock_client, mock_scenario, ["curve1", "curve2"]
    )

    assert result.success
    assert "curve1" in result.data
    assert "curve2" in result.data


def test_generic_curve_bulk_runner_partial_failure():
    mock_client = Mock(spec=BaseClient)
    mock_client.session = Mock()

    def mock_get(url):
        mock_response = Mock()
        if "curve1" in url:
            mock_response.ok = True
            mock_response.content = b"data1"
        else:
            mock_response.ok = False
            mock_response.status_code = 404
            mock_response.text = "Not Found"
        return mock_response

    mock_client.session.get.side_effect = mock_get

    mock_scenario = Mock()
    mock_scenario.id = 123

    result = GenericCurveBulkRunner.run(
        mock_client, mock_scenario, ["curve1", "curve2"]
    )

    assert result.success
    assert "curve1" in result.data
    assert result.errors
    assert any("curve2" in error for error in result.errors)


def test_generic_curve_bulk_runner_all_fail():
    mock_client = Mock(spec=BaseClient)
    mock_client.session = Mock()
    mock_response = Mock()
    mock_response.ok = False
    mock_response.status_code = 500
    mock_response.text = "Server Error"
    mock_client.session.get.return_value = mock_response

    mock_scenario = Mock()
    mock_scenario.id = 123

    result = GenericCurveBulkRunner.run(
        mock_client, mock_scenario, ["curve1", "curve2"]
    )

    assert not result.success
    assert len(result.errors) >= 2


def test_generic_curve_bulk_runner_unexpected_exception():
    mock_client = Mock(spec=BaseClient)
    mock_client.session = Mock()
    mock_client.session.get.side_effect = RuntimeError("Unexpected error")

    mock_scenario = Mock()
    mock_scenario.id = 123

    result = GenericCurveBulkRunner.run(mock_client, mock_scenario, ["curve1"])

    assert not result.success
    assert "Unexpected error" in result.errors[0]
