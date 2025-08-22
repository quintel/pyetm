import pandas as pd
import numpy as np
from pathlib import Path
from unittest.mock import Mock, patch
import pytest
from pyetm.models.custom_curves import CustomCurve, CustomCurves
from pyetm.services.scenario_runners.update_custom_curves import (
    UpdateCustomCurvesRunner,
)
from pyetm.services.service_result import ServiceResult


@pytest.fixture
def temp_curve_files():
    """Fixture that creates temporary curve files for testing"""
    temp_dir = Path("/tmp/test_update_curves")
    temp_dir.mkdir(exist_ok=True)

    files = {}

    # Create valid curve file (8760 values)
    valid_data = np.random.uniform(0, 100, 8760)
    valid_file = temp_dir / "valid_curve.csv"
    pd.Series(valid_data).to_csv(valid_file, header=False, index=False)
    files["valid"] = valid_file

    # Create another valid curve file
    another_data = np.random.uniform(50, 150, 8760)
    another_file = temp_dir / "another_curve.csv"
    pd.Series(another_data).to_csv(another_file, header=False, index=False)
    files["another"] = another_file

    yield files

    # Cleanup
    for file_path in files.values():
        file_path.unlink(missing_ok=True)
    temp_dir.rmdir()


def test_update_custom_curves_success_single_curve(temp_curve_files):
    """Test successful upload of a single custom curve"""
    mock_client = Mock()
    mock_client.session.base_url = "https://engine.example.com/api/v3"

    mock_scenario = Mock()
    mock_scenario.id = 12345

    curve = CustomCurve(
        key="test_curve", type="profile", file_path=temp_curve_files["valid"]
    )
    custom_curves = CustomCurves(curves=[curve])

    with patch.object(
        UpdateCustomCurvesRunner,
        "_make_batch_requests",
        return_value=[ServiceResult.ok(data={"status": "uploaded"})],
    ) as mock_batch:
        result = UpdateCustomCurvesRunner.run(mock_client, mock_scenario, custom_curves)

        assert result.success is True
        assert result.data["total_curves"] == 1
        assert result.data["successful_uploads"] == 1
        assert result.data["uploaded_curves"] == ["test_curve"]
        assert result.errors == []

    mock_batch.assert_called_once()
    call_client, request_list = mock_batch.call_args[0]
    assert call_client == mock_client
    assert len(request_list) == 1
    req = request_list[0]
    assert req["method"] == "put"
    assert f"/scenarios/{mock_scenario.id}/custom_curves/test_curve" in req["path"]
    assert req["kwargs"]["headers"]["Content-Type"] is None
    assert "files" in req["kwargs"]


def test_update_custom_curves_success_multiple_curves(temp_curve_files):
    """Test successful upload of multiple custom curves"""
    mock_client = Mock()
    mock_client.session.base_url = "https://engine.example.com/api/v3"

    mock_scenario = Mock()
    mock_scenario.id = 54321

    curves = [
        CustomCurve(key="curve_1", type="profile", file_path=temp_curve_files["valid"]),
        CustomCurve(
            key="curve_2", type="availability", file_path=temp_curve_files["another"]
        ),
    ]
    custom_curves = CustomCurves(curves=curves)

    with patch.object(
        UpdateCustomCurvesRunner,
        "_make_batch_requests",
        return_value=[
            ServiceResult.ok(data={"status": "uploaded"}),
            ServiceResult.ok(data={"status": "uploaded"}),
        ],
    ) as mock_batch:
        result = UpdateCustomCurvesRunner.run(mock_client, mock_scenario, custom_curves)

        assert result.success is True
        assert result.data["total_curves"] == 2
        assert result.data["successful_uploads"] == 2
        assert set(result.data["uploaded_curves"]) == {"curve_1", "curve_2"}
        assert result.errors == []

    mock_batch.assert_called_once()
    call_client, reqs = mock_batch.call_args[0]
    assert call_client == mock_client
    assert len(reqs) == 2


def test_update_custom_curves_curve_without_file():
    """Test upload of curve without file (uses contents() method)"""
    mock_client = Mock()
    mock_client.session.base_url = "https://engine.example.com/api/v3"

    mock_scenario = Mock()
    mock_scenario.id = 99999

    curve = CustomCurve(key="no_file_curve", type="profile")
    mock_series = pd.Series(np.random.uniform(0, 100, 8760))

    with patch(
        "pyetm.models.custom_curves.CustomCurve.contents", return_value=mock_series
    ):
        custom_curves = CustomCurves(curves=[curve])

        with patch.object(
            UpdateCustomCurvesRunner,
            "_make_batch_requests",
            return_value=[ServiceResult.ok(data={"status": "uploaded"})],
        ) as mock_batch:
            result = UpdateCustomCurvesRunner.run(
                mock_client, mock_scenario, custom_curves
            )

            assert result.success is True
            assert result.data["successful_uploads"] == 1
            assert result.data["uploaded_curves"] == ["no_file_curve"]

            mock_batch.assert_called_once()
            req = mock_batch.call_args[0][1][0]
            assert "files" in req["kwargs"]


def test_update_custom_curves_http_error():
    """Test handling of HTTP errors during upload"""
    mock_client = Mock()
    mock_client.session.base_url = "https://engine.example.com/api/v3"

    mock_scenario = Mock()
    mock_scenario.id = 12345

    curve = CustomCurve(key="error_curve", type="profile")
    mock_series = pd.Series(np.random.uniform(0, 100, 8760))

    with patch(
        "pyetm.models.custom_curves.CustomCurve.contents", return_value=mock_series
    ):
        custom_curves = CustomCurves(curves=[curve])

        with patch.object(
            UpdateCustomCurvesRunner,
            "_make_batch_requests",
            return_value=[ServiceResult.fail(["422: Validation failed"])],
        ):
            result = UpdateCustomCurvesRunner.run(
                mock_client, mock_scenario, custom_curves
            )

            assert result.success is False
            assert result.data["successful_uploads"] == 0
            assert len(result.errors) == 1
            assert result.errors[0] == "error_curve: 422: Validation failed"


def test_update_custom_curves_network_exception():
    """Test handling of network errors (simulated as failure result)"""
    mock_client = Mock()
    mock_client.session.base_url = "https://engine.example.com/api/v3"

    mock_scenario = Mock()
    mock_scenario.id = 12345

    curve = CustomCurve(key="network_error_curve", type="profile")
    mock_series = pd.Series(np.random.uniform(0, 100, 8760))

    with patch(
        "pyetm.models.custom_curves.CustomCurve.contents", return_value=mock_series
    ):
        custom_curves = CustomCurves(curves=[curve])

        with patch.object(
            UpdateCustomCurvesRunner,
            "_make_batch_requests",
            return_value=[ServiceResult.fail(["Network unreachable"])],
        ):
            result = UpdateCustomCurvesRunner.run(
                mock_client, mock_scenario, custom_curves
            )

            assert result.success is False
            assert result.data["successful_uploads"] == 0
            assert len(result.errors) == 1
            assert result.errors[0] == "network_error_curve: Network unreachable"


def test_update_custom_curves_mixed_success_failure(temp_curve_files):
    """Test upload with mix of successful and failed curves"""
    mock_client = Mock()
    mock_client.session.base_url = "https://engine.example.com/api/v3"

    mock_scenario = Mock()
    mock_scenario.id = 12345

    curves = [
        CustomCurve(
            key="success_curve", type="profile", file_path=temp_curve_files["valid"]
        ),
        CustomCurve(
            key="fail_curve", type="availability", file_path=temp_curve_files["another"]
        ),
    ]
    custom_curves = CustomCurves(curves=curves)

    with patch.object(
        UpdateCustomCurvesRunner,
        "_make_batch_requests",
        return_value=[
            ServiceResult.ok(data={"status": "uploaded"}),
            ServiceResult.fail(["500: Internal server error"]),
        ],
    ):
        result = UpdateCustomCurvesRunner.run(mock_client, mock_scenario, custom_curves)

        assert result.success is False
        assert result.data["total_curves"] == 2
        assert result.data["successful_uploads"] == 1
        assert result.data["uploaded_curves"] == ["success_curve"]
        assert len(result.errors) == 1
        assert result.errors[0] == "fail_curve: 500: Internal server error"


def test_update_custom_curves_empty_curves_list():
    """Test upload with empty curves list"""
    # Mock client
    mock_client = Mock()
    mock_client.session.base_url = "https://engine.example.com/api/v3"

    # Mock scenario
    mock_scenario = Mock()
    mock_scenario.id = 12345

    custom_curves = CustomCurves(curves=[])
    # Patch batch requests to avoid touching async machinery when list empty
    with patch.object(
        UpdateCustomCurvesRunner, "_make_batch_requests", return_value=[]
    ) as mock_batch:
        result = UpdateCustomCurvesRunner.run(mock_client, mock_scenario, custom_curves)

        # Should succeed with no uploads and not attempt any real batch calls
        assert result.success is True
        assert result.data["total_curves"] == 0
        assert result.data["successful_uploads"] == 0
        assert result.data["uploaded_curves"] == []
        assert len(result.errors) == 0
        mock_batch.assert_called_once()
        # Called with empty requests list
        call_client, reqs = mock_batch.call_args[0]
        assert call_client == mock_client
        assert reqs == []
