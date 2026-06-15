import pandas as pd
import io
import sys
from pathlib import Path
from unittest.mock import Mock, patch
from pyetm.models.hourly_output_curves import HourlyOutputCurve, HourlyOutputCurves
from pyetm.services.service_result import ServiceResult


def test_hourly_output_curve_retrieve_success():
    """Test successful curve retrieval and file saving"""
    mock_client = Mock()
    mock_scenario = Mock()
    mock_scenario.id = 123

    # Mock successful service result with CSV data
    csv_data = io.StringIO("hour,value\n0,1.5\n1,2.0\n2,1.8")
    mock_result = ServiceResult.ok(data=csv_data)

    with (
        patch(
            "pyetm.models.hourly_output_curves.DownloadHourlyOutputCurveRunner.run",
            return_value=mock_result,
        ),
        patch("pyetm.models.hourly_output_curves.get_settings") as mock_settings,
        patch("pandas.DataFrame.to_csv") as mock_to_csv,
    ):
        mock_settings.return_value.path_to_tmp.return_value = Path("/tmp/123")

        curve = HourlyOutputCurve(key="test_curve", type="output")
        result = curve.retrieve(mock_client, mock_scenario)

        assert isinstance(result, pd.DataFrame)
        assert curve.file_path is not None


def test_hourly_output_curve_retrieve_processing_error():
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
            "pyetm.models.hourly_output_curves.DownloadHourlyOutputCurveRunner.run",
            return_value=mock_result,
        ),
        patch("pyetm.models.hourly_output_curves.get_settings") as mock_settings,
    ):
        mock_settings.return_value.path_to_tmp.return_value = Path("/tmp/123")

        curve = HourlyOutputCurve(key="test_curve", type="output")
        result = curve.retrieve(mock_client, mock_scenario)

        assert result is None
        assert len(curve.warnings) > 0
        data_warnings = curve.warnings.get_by_field("data")
        assert len(data_warnings) > 0
        assert "Failed to process curve data" in data_warnings[0].message


def test_hourly_output_curve_retrieve_unexpected_error():
    """Test curve retrieval with unexpected exception"""
    mock_client = Mock()
    mock_scenario = Mock()
    mock_scenario.id = 123

    with patch(
        "pyetm.models.hourly_output_curves.DownloadHourlyOutputCurveRunner.run",
        side_effect=RuntimeError("Unexpected"),
    ):
        curve = HourlyOutputCurve(key="test_curve", type="output")
        result = curve.retrieve(mock_client, mock_scenario)

        assert result is None
        assert len(curve.warnings) > 0
        base_warnings = curve.warnings.get_by_field("base")
        assert len(base_warnings) > 0
        assert (
            "Unexpected error retrieving curve test_curve: Unexpected" in base_warnings[0].message
        )


def test_hourly_output_curve_contents_not_available():
    """Test contents when curve not available"""
    curve = HourlyOutputCurve(key="test_curve", type="output")
    result = curve.contents()

    assert result is None
    assert len(curve.warnings) > 0
    file_path_warnings = curve.warnings.get_by_field("file_path")
    assert len(file_path_warnings) > 0
    assert "not available - no file path set" in file_path_warnings[0].message


def test_hourly_output_curve_contents_file_error():
    """Test contents with file reading error"""
    curve = HourlyOutputCurve(
        key="test_curve", type="output", file_path=Path("/nonexistent/file.csv")
    )
    result = curve.contents()

    assert result is None
    assert len(curve.warnings) > 0
    file_path_warnings = curve.warnings.get_by_field("file_path")
    assert len(file_path_warnings) > 0
    assert "Failed to read curve file" in file_path_warnings[0].message


def test_hourly_output_curve_remove_not_available():
    """Test remove when no file path set"""
    curve = HourlyOutputCurve(key="test_curve", type="output")
    result = curve.remove()

    assert result is True


def test_hourly_output_curve_remove_file_error():
    """Test remove with file deletion error"""
    with patch("pathlib.Path.unlink", side_effect=OSError("Permission denied")):
        curve = HourlyOutputCurve(key="test_curve", type="output", file_path=Path("/test/file.csv"))
        result = curve.remove()

        assert result is False
        assert len(curve.warnings) > 0
        file_path_warnings = curve.warnings.get_by_field("file_path")
        assert len(file_path_warnings) > 0
        assert "Failed to remove curve file" in file_path_warnings[0].message


def test_hourly_output_curves_from_json_with_invalid_curve():
    """Test from_json with some invalid curve data"""
    data = [{"key": "valid_curve", "type": "carrier"}, {"invalid": "data"}]

    with patch.object(
        HourlyOutputCurve,
        "from_json",
        side_effect=[
            HourlyOutputCurve(key="valid_curve", type="output"),
            Exception("Invalid curve"),
        ],
    ):
        curves = HourlyOutputCurves.from_json(data)

        assert len(curves.curves) == 2  # 1 valid curve + 1 fallback curve
        assert len(curves.warnings) > 0
        # The key for the warnings appears to be based on the fallback curve that was created
        fallback_curve_key = (
            "HourlyOutputCurve(key=unknown).unknown"  # This is the actual key generated
        )
        fallback_curve_warnings = curves.warnings.get_by_field(fallback_curve_key)
        assert len(fallback_curve_warnings) > 0
        assert "Skipped invalid curve data" in fallback_curve_warnings[0].message


def test_hourly_output_curves_from_service_result_failure():
    """Test from_service_result with failed service result"""
    mock_scenario = Mock()
    mock_scenario.id = 123

    failed_result = ServiceResult.fail(errors=["API error", "Network error"])

    curves = HourlyOutputCurves.from_service_result(failed_result, mock_scenario)

    assert len(curves.curves) == 0
    base_warnings = curves.warnings.get_by_field("base")
    assert len(base_warnings) == 2
    warning_messages = [w.message for w in base_warnings]
    assert "Service error: API error" in warning_messages
    assert "Service error: Network error" in warning_messages


def test_hourly_output_curves_from_service_result_no_data():
    """Test from_service_result with successful result but no data"""
    mock_scenario = Mock()
    mock_scenario.id = 123

    empty_result = ServiceResult.ok(data=None)

    curves = HourlyOutputCurves.from_service_result(empty_result, mock_scenario)

    assert len(curves.curves) == 0


def test_hourly_output_curves_from_service_result_processing_error():
    """Test from_service_result with data processing error"""
    mock_scenario = Mock()
    mock_scenario.id = 123

    # Mock service result with curve data
    curve_data = io.StringIO("hour,value\n0,1.0")
    service_result = ServiceResult.ok(data={"test_curve": curve_data})

    with (
        patch("pyetm.models.hourly_output_curves.get_settings") as mock_settings,
        patch("pandas.read_csv", side_effect=Exception("CSV error")),
    ):
        mock_settings.return_value.path_to_tmp.return_value = Path("/tmp/123")

        curves = HourlyOutputCurves.from_service_result(service_result, mock_scenario)

        assert len(curves.curves) == 1
        assert curves.curves[0].key == "test_curve"
        assert curves.curves[0].type == "unknown"
        assert len(curves.curves[0].warnings) > 0
        base_warnings = curves.curves[0].warnings.get_by_field("base")
        assert len(base_warnings) > 0
        assert "Failed to process curve data" in base_warnings[0].message


def test_hourly_output_curves_from_service_result_no_caching():
    """Test from_service_result with cache_curves=False"""
    mock_scenario = Mock()
    mock_scenario.id = 123

    curve_data = io.StringIO("hour,value\n0,1.0")
    service_result = ServiceResult.ok(data={"test_curve": curve_data})

    curves = HourlyOutputCurves.from_service_result(
        service_result, mock_scenario, cache_curves=False
    )

    assert len(curves.curves) == 1
    assert curves.curves[0].key == "test_curve"
    assert curves.curves[0].file_path is None


def test_hourly_output_curves_infer_curve_type():
    """Test _infer_curve_type method"""
    assert HourlyOutputCurves._infer_curve_type("electricity_price") == "price_curve"
    assert HourlyOutputCurves._infer_curve_type("electricity_profiles") == "merit_curve"
    assert HourlyOutputCurves._infer_curve_type("electricity_capacities") == "capacity_curve"
    assert HourlyOutputCurves._infer_curve_type("unknown_curve") == "output_curve"
    # Test deprecated curve names (should still work with warning)
    assert HourlyOutputCurves._infer_curve_type("merit_order") == "merit_curve"


def test_hourly_output_curves_fetch_all():
    """Test fetch_all class method"""
    mock_scenario = Mock()
    mock_service_result = ServiceResult.ok(data={})
    mock_curves = HourlyOutputCurves(curves=[])

    with (
        patch("pyetm.models.hourly_output_curves.get_client") as mock_get_client,
        patch(
            "pyetm.models.hourly_output_curves.FetchAllHourlyOutputCurvesRunner"
        ) as mock_runner_class,
        patch.object(HourlyOutputCurves, "from_service_result") as mock_from_result,
    ):
        # Configure the mock runner to return our mock service result
        mock_runner_class.run.return_value = mock_service_result
        mock_from_result.return_value = mock_curves

        result = HourlyOutputCurves.fetch_all(mock_scenario)

        # Verify the runner was called with the correct arguments
        mock_runner_class.run.assert_called_once_with(mock_get_client.return_value, mock_scenario)

        # Verify from_service_result was called with the correct arguments
        mock_from_result.assert_called_once_with(mock_service_result, mock_scenario, True)

        assert result == mock_curves


def test_hourly_output_curves_create_empty_collection():
    """Test create_empty_collection class method"""
    # Create a mock for the FetchAllHourlyOutputCurvesRunner class
    mock_runner_class = Mock()
    mock_runner_class.CURVE_TYPES = ["curve1", "curve2"]

    # Mock the import that happens inside create_empty_collection
    with patch.dict(
        "sys.modules",
        {
            "pyetm.services.scenario_runners.fetch_hourly_output_curves": Mock(
                FetchAllHourlyOutputCurvesRunner=mock_runner_class
            )
        },
    ):
        curves = HourlyOutputCurves.create_empty_collection()

        assert len(curves.curves) == 2
        assert curves.curves[0].key == "curve1"
        assert curves.curves[1].key == "curve2"
        assert all(not curve.available() for curve in curves.curves)


def test_hourly_output_curves_to_dataframe_empty():
    """Test to_dataframe with no curves"""
    curves = HourlyOutputCurves(curves=[])
    df = curves.to_dataframe()

    assert isinstance(df, pd.DataFrame)
    assert df.empty
    assert list(df.index.names) == ["hour", "curve_name"]
    assert "value" in df.columns


def test_hourly_output_curves_to_dataframe_with_curves():
    """Test to_dataframe with available curves"""
    # Create mock curves with data
    curve1 = HourlyOutputCurve(key="curve1", type="output", file_path=Path("/tmp/curve1.csv"))
    curve2 = HourlyOutputCurve(key="curve2", type="output", file_path=Path("/tmp/curve2.csv"))

    # Mock the contents method to return test data
    curve1_data = pd.DataFrame({"value": [1.0, 2.0, 3.0]})
    curve2_data = pd.DataFrame({"value": [4.0, 5.0, 6.0]})

    with (
        patch.object(curve1, "contents", return_value=curve1_data),
        patch.object(curve2, "contents", return_value=curve2_data),
    ):
        curves = HourlyOutputCurves(curves=[curve1, curve2])
        df = curves.to_dataframe()

        # Check structure
        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert list(df.index.names) == ["hour", "curve_name"]
        assert "value" in df.columns

        # Check data
        assert len(df) == 6  # 3 hours * 2 curves
        assert df.loc[(0, "curve1"), "value"] == 1.0
        assert df.loc[(2, "curve2"), "value"] == 6.0


def test_hourly_output_curves_to_dataframe_filtered():
    """Test to_dataframe with curve filtering"""
    curve1 = HourlyOutputCurve(key="curve1", type="output", file_path=Path("/tmp/curve1.csv"))
    curve2 = HourlyOutputCurve(key="curve2", type="output", file_path=Path("/tmp/curve2.csv"))

    curve1_data = pd.DataFrame({"value": [1.0, 2.0]})
    curve2_data = pd.DataFrame({"value": [3.0, 4.0]})

    with (
        patch.object(curve1, "contents", return_value=curve1_data),
        patch.object(curve2, "contents", return_value=curve2_data),
    ):
        curves = HourlyOutputCurves(curves=[curve1, curve2])
        df = curves.to_dataframe(curves=["curve1"])

        # Only curve1 should be included
        assert len(df) == 2
        assert df.index.get_level_values("curve_name").unique().tolist() == ["curve1"]


def test_hourly_output_curves_to_dataframe_unavailable_skipped():
    """Test to_dataframe skips unavailable curves"""
    curve1 = HourlyOutputCurve(key="curve1", type="output")  # No file_path
    curve2 = HourlyOutputCurve(key="curve2", type="output", file_path=Path("/tmp/curve2.csv"))

    curve2_data = pd.DataFrame({"value": [1.0, 2.0]})

    with patch.object(curve2, "contents", return_value=curve2_data):
        curves = HourlyOutputCurves(curves=[curve1, curve2])
        df = curves.to_dataframe()

        # Only curve2 should be included
        assert len(df) == 2
        assert df.index.get_level_values("curve_name").unique().tolist() == ["curve2"]
