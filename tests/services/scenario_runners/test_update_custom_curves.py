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
    # Mock client
    mock_client = Mock()
    mock_client.session.base_url = "https://engine.example.com/api/v3"

    # Mock scenario
    mock_scenario = Mock()
    mock_scenario.id = 12345

    # Create custom curves with one curve
    curve = CustomCurve(
        key="test_curve", type="profile", file_path=temp_curve_files["valid"]
    )
    custom_curves = CustomCurves(curves=[curve])

    # Mock successful _make_request response
    with patch.object(
        UpdateCustomCurvesRunner,
        "_make_request",
        return_value=ServiceResult.ok(data={"status": "uploaded"}),
    ) as mock_make_request:

        result = UpdateCustomCurvesRunner.run(mock_client, mock_scenario, custom_curves)

        # Verify result
        assert result.success is True
        assert result.data["total_curves"] == 1
        assert result.data["successful_uploads"] == 1
        assert "test_curve" in result.data["uploaded_curves"]
        assert len(result.errors) == 0

        # Verify _make_request was called correctly
        mock_make_request.assert_called_once()
        call_args = mock_make_request.call_args
        assert call_args[1]["client"] == mock_client
        assert call_args[1]["method"] == "put"
        assert "/scenarios/12345/custom_curves/test_curve" in call_args[1]["path"]
        assert "files" in call_args[1]
        assert call_args[1]["headers"]["Content-Type"] is None


def test_update_custom_curves_success_multiple_curves(temp_curve_files):
    """Test successful upload of multiple custom curves"""
    # Mock client
    mock_client = Mock()
    mock_client.session.base_url = "https://engine.example.com/api/v3"

    # Mock scenario
    mock_scenario = Mock()
    mock_scenario.id = 54321

    # Create custom curves with multiple curves
    curves = [
        CustomCurve(key="curve_1", type="profile", file_path=temp_curve_files["valid"]),
        CustomCurve(
            key="curve_2", type="availability", file_path=temp_curve_files["another"]
        ),
    ]
    custom_curves = CustomCurves(curves=curves)

    # Mock successful _make_request responses
    with patch.object(
        UpdateCustomCurvesRunner,
        "_make_request",
        return_value=ServiceResult.ok(data={"status": "uploaded"}),
    ) as mock_make_request:

        result = UpdateCustomCurvesRunner.run(mock_client, mock_scenario, custom_curves)

        # Verify result
        assert result.success is True
        assert result.data["total_curves"] == 2
        assert result.data["successful_uploads"] == 2
        assert set(result.data["uploaded_curves"]) == {"curve_1", "curve_2"}
        assert len(result.errors) == 0

        # Verify _make_request was called twice
        assert mock_make_request.call_count == 2


def test_update_custom_curves_curve_without_file():
    """Test upload of curve without file (uses contents() method)"""
    # Mock client
    mock_client = Mock()
    mock_client.session.base_url = "https://engine.example.com/api/v3"

    # Mock scenario
    mock_scenario = Mock()
    mock_scenario.id = 99999

    # Create curve without file_path but with contents
    curve = CustomCurve(key="no_file_curve", type="profile")

    # Mock curve.contents() to return data
    mock_series = pd.Series(np.random.uniform(0, 100, 8760))

    with patch(
        "pyetm.models.custom_curves.CustomCurve.contents", return_value=mock_series
    ):
        custom_curves = CustomCurves(curves=[curve])

        # Mock successful _make_request response
        with patch.object(
            UpdateCustomCurvesRunner,
            "_make_request",
            return_value=ServiceResult.ok(data={"status": "uploaded"}),
        ) as mock_make_request:

            result = UpdateCustomCurvesRunner.run(
                mock_client, mock_scenario, custom_curves
            )

            # Verify result
            assert result.success is True
            assert result.data["successful_uploads"] == 1
            assert "no_file_curve" in result.data["uploaded_curves"]

            # Verify _make_request was called with file content
            mock_make_request.assert_called_once()
            call_args = mock_make_request.call_args
            assert "files" in call_args[1]


def test_update_custom_curves_http_error():
    """Test handling of HTTP errors during upload"""
    # Mock client
    mock_client = Mock()
    mock_client.session.base_url = "https://engine.example.com/api/v3"

    # Mock scenario
    mock_scenario = Mock()
    mock_scenario.id = 12345

    # Create custom curves
    curve = CustomCurve(key="error_curve", type="profile")
    mock_series = pd.Series(np.random.uniform(0, 100, 8760))

    with patch(
        "pyetm.models.custom_curves.CustomCurve.contents", return_value=mock_series
    ):
        custom_curves = CustomCurves(curves=[curve])

        # Mock _make_request failure response
        with patch.object(
            UpdateCustomCurvesRunner,
            "_make_request",
            return_value=ServiceResult.fail(["422: Validation failed"]),
        ) as mock_make_request:

            result = UpdateCustomCurvesRunner.run(
                mock_client, mock_scenario, custom_curves
            )

            # Verify result shows failure
            assert result.success is False
            assert result.data["successful_uploads"] == 0
            assert len(result.errors) == 1
            assert "422: Validation failed" in result.errors[0]


def test_update_custom_curves_network_exception():
    """Test handling of network exceptions during upload"""
    # Mock client
    mock_client = Mock()
    mock_client.session.base_url = "https://engine.example.com/api/v3"

    # Mock scenario
    mock_scenario = Mock()
    mock_scenario.id = 12345

    # Create custom curves
    curve = CustomCurve(key="network_error_curve", type="profile")
    mock_series = pd.Series(np.random.uniform(0, 100, 8760))

    with patch(
        "pyetm.models.custom_curves.CustomCurve.contents", return_value=mock_series
    ):
        custom_curves = CustomCurves(curves=[curve])

        # Mock _make_request raising exception
        with patch.object(
            UpdateCustomCurvesRunner,
            "_make_request",
            side_effect=ConnectionError("Network unreachable"),
        ):
            result = UpdateCustomCurvesRunner.run(
                mock_client, mock_scenario, custom_curves
            )

            # Verify result shows failure
            assert result.success is False
            assert result.data["successful_uploads"] == 0
            assert len(result.errors) == 1
            assert (
                "Error uploading network_error_curve: Network unreachable"
                in result.errors[0]
            )


def test_update_custom_curves_mixed_success_failure(temp_curve_files):
    """Test upload with mix of successful and failed curves"""
    # Mock client
    mock_client = Mock()
    mock_client.session.base_url = "https://engine.example.com/api/v3"

    # Mock scenario
    mock_scenario = Mock()
    mock_scenario.id = 12345

    # Create multiple curves
    curves = [
        CustomCurve(
            key="success_curve", type="profile", file_path=temp_curve_files["valid"]
        ),
        CustomCurve(
            key="fail_curve", type="availability", file_path=temp_curve_files["another"]
        ),
    ]
    custom_curves = CustomCurves(curves=curves)

    # Mock mixed responses (first succeeds, second fails)
    success_result = ServiceResult.ok(data={"status": "uploaded"})
    fail_result = ServiceResult.fail(["500: Internal server error"])

    with patch.object(
        UpdateCustomCurvesRunner,
        "_make_request",
        side_effect=[success_result, fail_result],
    ) as mock_make_request:

        result = UpdateCustomCurvesRunner.run(mock_client, mock_scenario, custom_curves)

        # Verify mixed result
        assert result.success is False
        assert result.data["total_curves"] == 2
        assert result.data["successful_uploads"] == 1
        assert result.data["uploaded_curves"] == ["success_curve"]
        assert len(result.errors) == 1
        assert "500: Internal server error" in result.errors[0]


def test_update_custom_curves_empty_curves_list():
    """Test upload with empty curves list"""
    # Mock client
    mock_client = Mock()
    mock_client.session.base_url = "https://engine.example.com/api/v3"

    # Mock scenario
    mock_scenario = Mock()
    mock_scenario.id = 12345

    custom_curves = CustomCurves(curves=[])

    result = UpdateCustomCurvesRunner.run(mock_client, mock_scenario, custom_curves)

    # Should succeed with no uploads
    assert result.success is True
    assert result.data["total_curves"] == 0
    assert result.data["successful_uploads"] == 0
    assert result.data["uploaded_curves"] == []
    assert len(result.errors) == 0
