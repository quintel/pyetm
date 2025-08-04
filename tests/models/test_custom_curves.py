import pandas as pd
import io
from pathlib import Path
from unittest.mock import Mock, patch
from pyetm.models.custom_curves import CustomCurve, CustomCurves
from pyetm.services.service_result import ServiceResult


def test_custom_curve_retrieve_success():
    """Test successful curve retrieval and file saving"""
    mock_client = Mock()
    mock_scenario = Mock()
    mock_scenario.id = 123

    # Mock successful service result
    csv_data = io.StringIO("1.0\n2.0\n3.0")
    mock_result = ServiceResult.ok(data=csv_data)

    with (
        patch(
            "pyetm.models.custom_curves.DownloadCustomCurveRunner.run",
            return_value=mock_result,
        ),
        patch("pyetm.models.custom_curves.get_settings") as mock_settings,
        patch("pandas.Series.to_csv") as mock_to_csv,
    ):

        mock_settings.return_value.path_to_tmp.return_value = Path("/tmp/123")

        curve = CustomCurve(key="test_curve", type="custom")
        result = curve.retrieve(mock_client, mock_scenario)

        assert isinstance(result, pd.Series)
        assert result.name == "test_curve"
        assert curve.file_path is not None


def test_custom_curve_retrieve_processing_error():
    """Test curve retrieval with data processing error"""
    mock_client = Mock()
    mock_scenario = Mock()
    mock_scenario.id = 123

    # Mock successful service result but bad data
    csv_data = io.StringIO("invalid,data")
    mock_result = ServiceResult.ok(data=csv_data)

    with (
        patch(
            "pyetm.models.custom_curves.DownloadCustomCurveRunner.run",
            return_value=mock_result,
        ),
        patch("pyetm.models.custom_curves.get_settings") as mock_settings,
    ):

        mock_settings.return_value.path_to_tmp.return_value = Path("/tmp/123")

        curve = CustomCurve(key="test_curve", type="custom")
        result = curve.retrieve(mock_client, mock_scenario)

        assert result is None
        assert len(curve.warnings) > 0
        key_warnings = curve.warnings.get_by_field(curve.key)
        assert len(key_warnings) > 0
        assert "Failed to process curve data" in key_warnings[0].message


def test_custom_curve_retrieve_service_error():
    """Test curve retrieval with service error"""
    mock_client = Mock()
    mock_scenario = Mock()
    mock_scenario.id = 123

    # Mock failed service result
    mock_result = ServiceResult.fail(errors=["API error"])

    with patch(
        "pyetm.models.custom_curves.DownloadCustomCurveRunner.run",
        return_value=mock_result,
    ):
        curve = CustomCurve(key="test_curve", type="custom")
        result = curve.retrieve(mock_client, mock_scenario)

        assert result is None
        assert len(curve.warnings) > 0
        key_warnings = curve.warnings.get_by_field(curve.key)
        assert len(key_warnings) > 0
        assert "Failed to retrieve curve: API error" in key_warnings[0].message


def test_custom_curve_retrieve_unexpected_error():
    """Test curve retrieval with unexpected exception"""
    mock_client = Mock()
    mock_scenario = Mock()
    mock_scenario.id = 123

    with patch(
        "pyetm.models.custom_curves.DownloadCustomCurveRunner.run",
        side_effect=RuntimeError("Unexpected"),
    ):
        curve = CustomCurve(key="test_curve", type="custom")
        result = curve.retrieve(mock_client, mock_scenario)

        assert result is None
        assert len(curve.warnings) > 0
        key_warnings = curve.warnings.get_by_field(curve.key)
        assert len(key_warnings) > 0
        assert (
            "Unexpected error retrieving curve: Unexpected" in key_warnings[0].message
        )


def test_custom_curve_contents_not_available():
    """Test contents when curve not available"""
    curve = CustomCurve(key="test_curve", type="custom")
    result = curve.contents()

    assert result is None
    assert len(curve.warnings) > 0
    key_warnings = curve.warnings.get_by_field(curve.key)
    assert len(key_warnings) > 0
    assert "not available - no file path set" in key_warnings[0].message


def test_custom_curve_contents_file_error():
    """Test contents with file reading error"""
    curve = CustomCurve(
        key="test_curve", type="custom", file_path=Path("/nonexistent/file.csv")
    )
    result = curve.contents()

    assert result is None
    assert len(curve.warnings) > 0
    key_warnings = curve.warnings.get_by_field(curve.key)
    assert len(key_warnings) > 0
    assert "Failed to read curve file" in key_warnings[0].message


def test_custom_curve_remove_not_available():
    """Test remove when no file path set"""
    curve = CustomCurve(key="test_curve", type="custom")
    result = curve.remove()

    assert result is True


def test_custom_curve_remove_file_error():
    """Test remove with file deletion error"""
    with patch("pathlib.Path.unlink", side_effect=OSError("Permission denied")):
        curve = CustomCurve(
            key="test_curve", type="custom", file_path=Path("/test/file.csv")
        )
        result = curve.remove()

        assert result is False
        assert len(curve.warnings) > 0
        key_warnings = curve.warnings.get_by_field(curve.key)
        assert len(key_warnings) > 0
        assert "Failed to remove curve file" in key_warnings[0].message


def test_custom_curves_from_json_with_invalid_curve():
    """Test from_json with some invalid curve data"""
    data = [{"key": "valid_curve", "type": "custom"}, {"invalid": "data"}]

    with patch.object(
        CustomCurve,
        "from_json",
        side_effect=[
            CustomCurve(key="valid_curve", type="custom"),
            Exception("Invalid curve"),
        ],
    ):
        curves = CustomCurves.from_json(data)

        assert len(curves.curves) == 2  # 1 valid curve + 1 fallback curve
        assert len(curves.warnings) > 0
        # The key for the warnings appears to be based on the fallback curve that was created
        fallback_curve_key = (
            "CustomCurve(key=unknown).unknown"  # This is the actual key generated
        )
        fallback_curve_warnings = curves.warnings.get_by_field(fallback_curve_key)
        assert len(fallback_curve_warnings) > 0
        assert "Skipped invalid curve data" in fallback_curve_warnings[0].message
