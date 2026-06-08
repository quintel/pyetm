from unittest.mock import Mock, patch
import pytest
from pyetm.services.scenario_runners.delete_custom_curves import (
    DeleteCustomCurvesRunner,
)
from pyetm.services.service_result import ServiceResult


def test_delete_custom_curves_success_single_curve():
    """Test successful deletion of a single custom curve"""
    mock_client = Mock()
    mock_client.session.base_url = "https://engine.example.com/api/v3"

    mock_scenario = Mock()
    mock_scenario.id = 12345

    curve_keys = ["test_curve"]

    with patch.object(
        DeleteCustomCurvesRunner,
        "_make_batch_requests",
        return_value=[ServiceResult.ok(data={"status": "deleted"})],
    ) as mock_batch:
        result = DeleteCustomCurvesRunner.run(mock_client, mock_scenario, curve_keys)

        assert result.success is True
        assert result.data["total_curves"] == 1
        assert result.data["successful_deletions"] == 1
        assert result.data["deleted_curves"] == ["test_curve"]
        assert result.errors == []

    mock_batch.assert_called_once()
    call_client, request_list = mock_batch.call_args[0]
    assert call_client == mock_client
    assert len(request_list) == 1
    req = request_list[0]
    assert req["method"] == "delete"
    assert f"/scenarios/{mock_scenario.id}/custom_curves/test_curve" in req["path"]
    assert req["payload"] is None


def test_delete_custom_curves_success_multiple_curves():
    """Test successful deletion of multiple custom curves"""
    mock_client = Mock()
    mock_client.session.base_url = "https://engine.example.com/api/v3"

    mock_scenario = Mock()
    mock_scenario.id = 54321

    curve_keys = ["curve_1", "curve_2", "curve_3"]

    with patch.object(
        DeleteCustomCurvesRunner,
        "_make_batch_requests",
        return_value=[
            ServiceResult.ok(data={"status": "deleted"}),
            ServiceResult.ok(data={"status": "deleted"}),
            ServiceResult.ok(data={"status": "deleted"}),
        ],
    ) as mock_batch:
        result = DeleteCustomCurvesRunner.run(mock_client, mock_scenario, curve_keys)

        assert result.success is True
        assert result.data["total_curves"] == 3
        assert result.data["successful_deletions"] == 3
        assert set(result.data["deleted_curves"]) == {"curve_1", "curve_2", "curve_3"}
        assert result.errors == []

    mock_batch.assert_called_once()
    call_client, reqs = mock_batch.call_args[0]
    assert call_client == mock_client
    assert len(reqs) == 3
    # Verify each request structure
    for req in reqs:
        assert req["method"] == "delete"
        assert "/custom_curves/" in req["path"]
        assert req["payload"] is None


def test_delete_custom_curves_empty_list():
    """Test deletion with empty curve keys list"""
    mock_client = Mock()
    mock_client.session.base_url = "https://engine.example.com/api/v3"

    mock_scenario = Mock()
    mock_scenario.id = 12345

    curve_keys = []

    result = DeleteCustomCurvesRunner.run(mock_client, mock_scenario, curve_keys)

    # Should succeed immediately without making any API calls
    assert result.success is True
    assert result.data["total_curves"] == 0
    assert result.data["successful_deletions"] == 0
    assert result.data["deleted_curves"] == []
    assert len(result.errors) == 0


def test_delete_custom_curves_with_set():
    """Test deletion with set of curve keys instead of list"""
    mock_client = Mock()
    mock_client.session.base_url = "https://engine.example.com/api/v3"

    mock_scenario = Mock()
    mock_scenario.id = 99999

    curve_keys = {"curve_a", "curve_b"}

    with patch.object(
        DeleteCustomCurvesRunner,
        "_make_batch_requests",
        return_value=[
            ServiceResult.ok(data={"status": "deleted"}),
            ServiceResult.ok(data={"status": "deleted"}),
        ],
    ):
        result = DeleteCustomCurvesRunner.run(mock_client, mock_scenario, curve_keys)

        assert result.success is True
        assert result.data["total_curves"] == 2
        assert result.data["successful_deletions"] == 2
        assert set(result.data["deleted_curves"]) == {"curve_a", "curve_b"}


def test_delete_custom_curves_http_404_error():
    """Test handling of 404 errors (curve not found)"""
    mock_client = Mock()
    mock_client.session.base_url = "https://engine.example.com/api/v3"

    mock_scenario = Mock()
    mock_scenario.id = 12345

    curve_keys = ["nonexistent_curve"]

    with patch.object(
        DeleteCustomCurvesRunner,
        "_make_batch_requests",
        return_value=[ServiceResult.fail(["404: Curve not found"])],
    ):
        result = DeleteCustomCurvesRunner.run(mock_client, mock_scenario, curve_keys)

        assert result.success is False
        assert result.data["successful_deletions"] == 0
        assert len(result.errors) == 1
        assert result.errors[0] == "nonexistent_curve: 404: Curve not found"


def test_delete_custom_curves_http_422_error():
    """Test handling of 422 validation errors"""
    mock_client = Mock()
    mock_client.session.base_url = "https://engine.example.com/api/v3"

    mock_scenario = Mock()
    mock_scenario.id = 12345

    curve_keys = ["invalid_curve"]

    with patch.object(
        DeleteCustomCurvesRunner,
        "_make_batch_requests",
        return_value=[ServiceResult.fail(["422: Validation failed"])],
    ):
        result = DeleteCustomCurvesRunner.run(mock_client, mock_scenario, curve_keys)

        assert result.success is False
        assert result.data["successful_deletions"] == 0
        assert len(result.errors) == 1
        assert result.errors[0] == "invalid_curve: 422: Validation failed"


def test_delete_custom_curves_http_500_error():
    """Test handling of 500 server errors"""
    mock_client = Mock()
    mock_client.session.base_url = "https://engine.example.com/api/v3"

    mock_scenario = Mock()
    mock_scenario.id = 12345

    curve_keys = ["server_error_curve"]

    with patch.object(
        DeleteCustomCurvesRunner,
        "_make_batch_requests",
        return_value=[ServiceResult.fail(["500: Internal server error"])],
    ):
        result = DeleteCustomCurvesRunner.run(mock_client, mock_scenario, curve_keys)

        assert result.success is False
        assert result.data["successful_deletions"] == 0
        assert len(result.errors) == 1
        assert result.errors[0] == "server_error_curve: 500: Internal server error"


def test_delete_custom_curves_network_exception():
    """Test handling of network errors"""
    mock_client = Mock()
    mock_client.session.base_url = "https://engine.example.com/api/v3"

    mock_scenario = Mock()
    mock_scenario.id = 12345

    curve_keys = ["network_error_curve"]

    with patch.object(
        DeleteCustomCurvesRunner,
        "_make_batch_requests",
        return_value=[ServiceResult.fail(["Network unreachable"])],
    ):
        result = DeleteCustomCurvesRunner.run(mock_client, mock_scenario, curve_keys)

        assert result.success is False
        assert result.data["successful_deletions"] == 0
        assert len(result.errors) == 1
        assert result.errors[0] == "network_error_curve: Network unreachable"


def test_delete_custom_curves_mixed_success_failure():
    """Test deletion with mix of successful and failed curves"""
    mock_client = Mock()
    mock_client.session.base_url = "https://engine.example.com/api/v3"

    mock_scenario = Mock()
    mock_scenario.id = 12345

    curve_keys = ["success_curve_1", "fail_curve", "success_curve_2"]

    with patch.object(
        DeleteCustomCurvesRunner,
        "_make_batch_requests",
        return_value=[
            ServiceResult.ok(data={"status": "deleted"}),
            ServiceResult.fail(["404: Curve not found"]),
            ServiceResult.ok(data={"status": "deleted"}),
        ],
    ):
        result = DeleteCustomCurvesRunner.run(mock_client, mock_scenario, curve_keys)

        assert result.success is False  # Overall failure due to partial failure
        assert result.data["total_curves"] == 3
        assert result.data["successful_deletions"] == 2
        assert set(result.data["deleted_curves"]) == {"success_curve_1", "success_curve_2"}
        assert len(result.errors) == 1
        assert result.errors[0] == "fail_curve: 404: Curve not found"


def test_delete_custom_curves_all_failures():
    """Test deletion where all curves fail"""
    mock_client = Mock()
    mock_client.session.base_url = "https://engine.example.com/api/v3"

    mock_scenario = Mock()
    mock_scenario.id = 12345

    curve_keys = ["fail_1", "fail_2"]

    with patch.object(
        DeleteCustomCurvesRunner,
        "_make_batch_requests",
        return_value=[
            ServiceResult.fail(["404: Not found"]),
            ServiceResult.fail(["500: Server error"]),
        ],
    ):
        result = DeleteCustomCurvesRunner.run(mock_client, mock_scenario, curve_keys)

        assert result.success is False
        assert result.data["total_curves"] == 2
        assert result.data["successful_deletions"] == 0
        assert result.data["deleted_curves"] == []
        assert len(result.errors) == 2
        assert "fail_1: 404: Not found" in result.errors
        assert "fail_2: 500: Server error" in result.errors


def test_delete_custom_curves_request_structure():
    """Test that requests are structured correctly for the API"""
    mock_client = Mock()
    mock_client.session.base_url = "https://engine.example.com/api/v3"

    mock_scenario = Mock()
    mock_scenario.id = 77777

    curve_keys = ["weather/solar_pv", "weather/wind_offshore"]

    with patch.object(
        DeleteCustomCurvesRunner,
        "_make_batch_requests",
        return_value=[
            ServiceResult.ok(data={}),
            ServiceResult.ok(data={}),
        ],
    ) as mock_batch:
        DeleteCustomCurvesRunner.run(mock_client, mock_scenario, curve_keys)

        mock_batch.assert_called_once()
        call_client, requests = mock_batch.call_args[0]

        assert call_client == mock_client
        assert len(requests) == 2

        # Verify first request
        assert requests[0]["method"] == "delete"
        assert requests[0]["path"] == f"/scenarios/77777/custom_curves/weather/solar_pv"
        assert requests[0]["payload"] is None

        # Verify second request
        assert requests[1]["method"] == "delete"
        assert requests[1]["path"] == f"/scenarios/77777/custom_curves/weather/wind_offshore"
        assert requests[1]["payload"] is None


def test_delete_custom_curves_multiple_errors_per_curve():
    """Test handling when a curve deletion returns multiple error messages"""
    mock_client = Mock()
    mock_client.session.base_url = "https://engine.example.com/api/v3"

    mock_scenario = Mock()
    mock_scenario.id = 12345

    curve_keys = ["multi_error_curve"]

    with patch.object(
        DeleteCustomCurvesRunner,
        "_make_batch_requests",
        return_value=[
            ServiceResult.fail([
                "Error 1: Validation failed",
                "Error 2: Permission denied",
            ])
        ],
    ):
        result = DeleteCustomCurvesRunner.run(mock_client, mock_scenario, curve_keys)

        assert result.success is False
        assert result.data["successful_deletions"] == 0
        assert len(result.errors) == 2
        assert "multi_error_curve: Error 1: Validation failed" in result.errors
        assert "multi_error_curve: Error 2: Permission denied" in result.errors
