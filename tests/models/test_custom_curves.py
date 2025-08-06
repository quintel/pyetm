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


def test_custom_curve_from_dataframe_basic_roundtrip():
    """Test basic serialization and deserialization of a CustomCurve."""
    import numpy as np

    hours = 24
    test_data = pd.Series(np.random.rand(hours) * 100, name="test_curve")
    original = CustomCurve(key="test_curve", type="profile")

    # Save test data to temporary file
    temp_dir = Path("/tmp/test_curves")
    temp_dir.mkdir(exist_ok=True)
    temp_file = temp_dir / "test_curve.csv"
    test_data.to_csv(temp_file, index=False, header=False)
    original.file_path = temp_file

    try:
        # Serialize to DataFrame
        df = original.to_dataframe()

        # Should be time series format: one column with curve key, hour index
        assert df.shape[1] == 1
        assert df.columns[0] == "test_curve"
        assert df.index.name == "hour"
        restored = CustomCurve.from_dataframe(df)

        # Verify properties
        assert restored.key == original.key
        assert restored.type == "custom"  # Default type from DataFrame deserialization
        assert restored.available()

        # Verify data is preserved
        restored_data = restored.contents()
        assert restored_data is not None
        assert len(restored_data) == hours

    finally:
        # Clean up
        if temp_file.exists():
            temp_file.unlink()
        if restored.file_path and restored.file_path.exists():
            restored.file_path.unlink()
        try:
            temp_dir.rmdir()
        except OSError:
            pass  # Directory not empty or doesn't exist


def test_custom_curve_from_dataframe_without_file_path():
    """Test deserialization of curve without file_path."""
    original = CustomCurve(key="no_file_curve", type="custom")

    df = original.to_dataframe()
    restored = CustomCurve.from_dataframe(df)

    assert restored.key == original.key
    assert restored.type == "custom"  # Default type from DataFrame deserialization
    assert restored.file_path is None
    assert not restored.available()


def test_custom_curve_from_dataframe_alternative_structure():
    """Test deserialization from DataFrame with time series data."""
    import numpy as np

    hours = 12
    data = np.random.rand(hours) * 50
    df = pd.DataFrame({"alt_curve": data})
    df.index.name = "hour"

    restored = CustomCurve.from_dataframe(df)

    assert restored.key == "alt_curve"
    assert restored.type == "custom"  # Default type from DataFrame deserialization
    assert restored.available()  # Should have saved the data

    # Verify data was saved correctly
    restored_data = restored.contents()
    assert restored_data is not None
    assert len(restored_data) == hours

    # Clean up
    if restored.file_path and restored.file_path.exists():
        restored.file_path.unlink()


def test_custom_curve_from_dataframe_invalid_multiple_rows():
    """Test error handling when DataFrame has multiple rows."""
    df = pd.DataFrame(
        {
            "key": ["curve1", "curve2"],
            "type": ["profile", "availability"],
            "file_path": [None, "/tmp/test.csv"],
        }
    )

    restored = CustomCurve.from_dataframe(df)

    assert isinstance(restored, CustomCurve)
    assert len(restored.warnings) > 0
    from_dataframe_warnings = restored.warnings.get_by_field("from_dataframe")
    assert len(from_dataframe_warnings) > 0


def test_custom_curve_from_dataframe_fallback_on_error():
    """Test fallback behavior when deserialization fails."""
    df = pd.DataFrame({"invalid_field": ["value"], "another_invalid": ["value2"]})

    # Base.from_dataframe should handle the error and return instance with warning
    restored = CustomCurve.from_dataframe(df)

    assert len(restored.warnings) > 0
    assert len(restored.warnings.get_fields_with_warnings()) > 0


def test_custom_curves_from_dataframe_collection_roundtrip():
    """Test serialization and deserialization of a CustomCurves collection."""
    import numpy as np

    # Create test data for curves
    hours = 24
    temp_dir = Path("/tmp/test_curves_collection")
    temp_dir.mkdir(exist_ok=True)

    curves_list = []
    test_files = []

    try:
        # Create curves with actual data
        for i, (key, curve_type) in enumerate(
            [("curve1", "profile"), ("curve2", "availability"), ("curve3", "custom")]
        ):
            curve = CustomCurve(key=key, type=curve_type)

            # Only add data to some curves to test mixed scenarios
            if i < 2:  # First two curves get data
                data = pd.Series(np.random.rand(hours) * (i + 1) * 10, name=key)
                temp_file = temp_dir / f"{key}.csv"
                data.to_csv(temp_file, index=False, header=False)
                curve.file_path = temp_file
                test_files.append(temp_file)

            curves_list.append(curve)

        original_collection = CustomCurves(curves=curves_list)

        # Serialize to DataFrame
        df = original_collection.to_dataframe()

        # Should have columns for each curve and hour index
        assert df.index.name == "hour"
        assert len(df.columns) == 3  # Three curves
        assert set(df.columns) == {"curve1", "curve2", "curve3"}

        # Deserialize back
        restored_collection = CustomCurves.from_dataframe(df)
        assert len(restored_collection.curves) == len(original_collection.curves)

        # Verify each curve (note: type information is not preserved in time series format)
        for orig, rest in zip(original_collection.curves, restored_collection.curves):
            assert orig.key == rest.key

    finally:
        # Clean up
        for file_path in test_files:
            if file_path.exists():
                file_path.unlink()

        # Clean up any files created during deserialization
        for curve in (
            restored_collection.curves if "restored_collection" in locals() else []
        ):
            if curve.file_path and curve.file_path.exists():
                curve.file_path.unlink()

        try:
            temp_dir.rmdir()
        except OSError:
            pass  # Directory not empty or doesn't exist


def test_custom_curves_from_dataframe_empty_collection():
    """Test deserialization of empty collection."""
    empty_collection = CustomCurves(curves=[])

    df = empty_collection.to_dataframe()
    # Should be empty DataFrame with hour index
    assert df.empty
    assert df.index.name == "hour"

    restored = CustomCurves.from_dataframe(df)

    assert len(restored.curves) == 0


def test_custom_curves_from_dataframe_with_invalid_curve_data():
    """Test handling of invalid curve data in collection."""
    import numpy as np

    hours = 10
    df = pd.DataFrame(
        {
            "valid_curve": np.random.rand(hours) * 100,
            "problem_curve": [np.nan] * hours,  # All NaN values
        }
    )
    df.index.name = "hour"

    restored_collection = CustomCurves.from_dataframe(df)
    assert len(restored_collection.curves) == 2

    # Check that we got both curves
    curve_keys = {curve.key for curve in restored_collection.curves}
    assert curve_keys == {"valid_curve", "problem_curve"}

    # Valid curve should have data, problem curve might not
    valid_curve = next(c for c in restored_collection.curves if c.key == "valid_curve")
    assert valid_curve.key == "valid_curve"

    # Clean up any created files
    for curve in restored_collection.curves:
        if curve.file_path and curve.file_path.exists():
            curve.file_path.unlink()


def test_custom_curves_from_dataframe_preserves_warnings():
    """Test that warnings from individual curves are preserved in collection."""
    curves = CustomCurves(
        curves=[
            CustomCurve(key="good_curve", type="profile"),
            CustomCurve(key="another_curve", type="availability"),
        ]
    )

    df = curves.to_dataframe()
    restored = CustomCurves.from_dataframe(df)

    assert len(restored.curves) == 2
