import pandas as pd
import io
import sys
from pathlib import Path
from unittest.mock import Mock, patch
from pyetm.models.output_curves import OutputCurve, OutputCurves
from pyetm.services.service_result import ServiceResult


def test_output_curve_retrieve_success():
    """Test successful curve retrieval and file saving"""
    mock_client = Mock()
    mock_scenario = Mock()
    mock_scenario.id = 123

    # Mock successful service result with CSV data
    csv_data = io.StringIO("hour,value\n0,1.5\n1,2.0\n2,1.8")
    mock_result = ServiceResult.ok(data=csv_data)

    with (
        patch(
            "pyetm.models.output_curves.DownloadOutputCurveRunner.run",
            return_value=mock_result,
        ),
        patch("pyetm.models.output_curves.get_settings") as mock_settings,
        patch("pandas.DataFrame.to_csv") as mock_to_csv,
    ):

        mock_settings.return_value.path_to_tmp.return_value = Path("/tmp/123")

        curve = OutputCurve(key="test_curve", type="output")
        result = curve.retrieve(mock_client, mock_scenario)

        assert isinstance(result, pd.DataFrame)
        assert curve.file_path is not None


def test_output_curve_retrieve_processing_error():
    """Test curve retrieval with data processing error"""
    mock_client = Mock()
    mock_scenario = Mock()
    mock_scenario.id = 123

    # Mock successful service result but with data that will cause pandas error
    csv_data = io.StringIO("invalid,csv,data")
    csv_data.seek = Mock(side_effect=Exception("Seek error"))
    mock_result = ServiceResult.ok(data=csv_data)

    with (
        patch(
            "pyetm.models.output_curves.DownloadOutputCurveRunner.run",
            return_value=mock_result,
        ),
        patch("pyetm.models.output_curves.get_settings") as mock_settings,
    ):

        mock_settings.return_value.path_to_tmp.return_value = Path("/tmp/123")

        curve = OutputCurve(key="test_curve", type="output")
        result = curve.retrieve(mock_client, mock_scenario)

        assert result is None
        assert len(curve.warnings) > 0
        assert "Failed to process curve data" in curve.warnings['data'][0]


def test_output_curve_retrieve_unexpected_error():
    """Test curve retrieval with unexpected exception"""
    mock_client = Mock()
    mock_scenario = Mock()
    mock_scenario.id = 123

    with patch(
        "pyetm.models.output_curves.DownloadOutputCurveRunner.run",
        side_effect=RuntimeError("Unexpected"),
    ):
        curve = OutputCurve(key="test_curve", type="output")
        result = curve.retrieve(mock_client, mock_scenario)

        assert result is None
        assert len(curve.warnings) > 0
        assert (
            "Unexpected error retrieving curve test_curve: Unexpected"
            in curve.warnings['base'][0]
        )


def test_output_curve_contents_not_available():
    """Test contents when curve not available"""
    curve = OutputCurve(key="test_curve", type="output")
    result = curve.contents()

    assert result is None
    assert len(curve.warnings) > 0
    assert "not available - no file path set" in curve.warnings['file_path'][0]


def test_output_curve_contents_file_error():
    """Test contents with file reading error"""
    curve = OutputCurve(
        key="test_curve", type="output", file_path=Path("/nonexistent/file.csv")
    )
    result = curve.contents()

    assert result is None
    assert len(curve.warnings) > 0
    assert "Failed to read curve file" in curve.warnings['file_path'][0]


def test_output_curve_remove_not_available():
    """Test remove when no file path set"""
    curve = OutputCurve(key="test_curve", type="output")
    result = curve.remove()

    assert result is True


def test_output_curve_remove_file_error():
    """Test remove with file deletion error"""
    with patch("pathlib.Path.unlink", side_effect=OSError("Permission denied")):
        curve = OutputCurve(
            key="test_curve", type="output", file_path=Path("/test/file.csv")
        )
        result = curve.remove()

        assert result is False
        assert len(curve.warnings) > 0
        assert "Failed to remove curve file" in curve.warnings['file_path'][0]


def test_output_curves_from_json_with_invalid_curve():
    """Test from_json with some invalid curve data"""
    data = [{"key": "valid_curve", "type": "carrier"}, {"invalid": "data"}]

    with patch.object(
        OutputCurve,
        "from_json",
        side_effect=[
            OutputCurve(key="valid_curve", type="output"),
            Exception("Invalid curve"),
        ],
    ):
        curves = OutputCurves.from_json(data)

        assert len(curves.curves) == 1
        assert len(curves.warnings) > 0
        assert "Skipped invalid curve data" in curves.warnings['OutputCurve(key=valid_curve)'][0]


def test_output_curves_from_service_result_failure():
    """Test from_service_result with failed service result"""
    mock_scenario = Mock()
    mock_scenario.id = 123

    failed_result = ServiceResult.fail(errors=["API error", "Network error"])

    curves = OutputCurves.from_service_result(failed_result, mock_scenario)

    assert len(curves.curves) == 0
    assert len(curves.warnings['base']) == 2
    assert "Service error: API error" in curves.warnings['base'][0]
    assert "Service error: Network error" in curves.warnings['base'][1]


def test_output_curves_from_service_result_no_data():
    """Test from_service_result with successful result but no data"""
    mock_scenario = Mock()
    mock_scenario.id = 123

    empty_result = ServiceResult.ok(data=None)

    curves = OutputCurves.from_service_result(empty_result, mock_scenario)

    assert len(curves.curves) == 0


def test_output_curves_from_service_result_processing_error():
    """Test from_service_result with data processing error"""
    mock_scenario = Mock()
    mock_scenario.id = 123

    # Mock service result with curve data
    curve_data = io.StringIO("hour,value\n0,1.0")
    service_result = ServiceResult.ok(data={"test_curve": curve_data})

    with (
        patch("pyetm.models.output_curves.get_settings") as mock_settings,
        patch("pandas.read_csv", side_effect=Exception("CSV error")),
    ):

        mock_settings.return_value.path_to_tmp.return_value = Path("/tmp/123")

        curves = OutputCurves.from_service_result(service_result, mock_scenario)

        assert len(curves.curves) == 1
        assert curves.curves[0].key == "test_curve"
        assert curves.curves[0].type == "unknown"
        assert len(curves.curves[0].warnings) > 0
        assert "Failed to process curve data" in curves.curves[0].warnings['base'][0]


def test_output_curves_from_service_result_no_caching():
    """Test from_service_result with cache_curves=False"""
    mock_scenario = Mock()
    mock_scenario.id = 123

    curve_data = io.StringIO("hour,value\n0,1.0")
    service_result = ServiceResult.ok(data={"test_curve": curve_data})

    curves = OutputCurves.from_service_result(
        service_result, mock_scenario, cache_curves=False
    )

    assert len(curves.curves) == 1
    assert curves.curves[0].key == "test_curve"
    assert curves.curves[0].file_path is None


def test_output_curves_infer_curve_type():
    """Test _infer_curve_type method"""
    assert OutputCurves._infer_curve_type("electricity_price") == "price_curve"
    assert OutputCurves._infer_curve_type("merit_order") == "merit_curve"
    assert OutputCurves._infer_curve_type("unknown_curve") == "output_curve"


def test_output_curves_fetch_all():
    """Test fetch_all class method"""
    mock_scenario = Mock()
    mock_service_result = ServiceResult.ok(data={})
    mock_curves = OutputCurves(curves=[])

    with (
        patch("pyetm.models.output_curves.BaseClient") as mock_client_class,
        patch(
            "pyetm.models.output_curves.FetchAllOutputCurvesRunner"
        ) as mock_runner_class,
        patch.object(OutputCurves, "from_service_result") as mock_from_result,
    ):
        # Configure the mock runner to return our mock service result
        mock_runner_class.run.return_value = mock_service_result
        mock_from_result.return_value = mock_curves

        result = OutputCurves.fetch_all(mock_scenario)

        # Verify the runner was called with the correct arguments
        mock_runner_class.run.assert_called_once_with(
            mock_client_class.return_value, mock_scenario
        )

        # Verify from_service_result was called with the correct arguments
        mock_from_result.assert_called_once_with(
            mock_service_result, mock_scenario, True
        )

        assert result == mock_curves


def test_output_curves_create_empty_collection():
    """Test create_empty_collection class method"""
    # Create a mock for the FetchAllOutputCurvesRunner class
    mock_runner_class = Mock()
    mock_runner_class.CURVE_TYPES = ["curve1", "curve2"]

    # Mock the import that happens inside create_empty_collection
    with patch.dict(
        "sys.modules",
        {
            "pyetm.services.scenario_runners.fetch_output_curves": Mock(
                FetchAllOutputCurvesRunner=mock_runner_class
            )
        },
    ):
        curves = OutputCurves.create_empty_collection()

        assert len(curves.curves) == 2
        assert curves.curves[0].key == "curve1"
        assert curves.curves[1].key == "curve2"
        assert all(not curve.available() for curve in curves.curves)
