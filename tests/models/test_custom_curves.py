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


def test_custom_curve_remove_success(tmp_path):
    """Remove should delete file and clear file_path when available."""
    temp_file = tmp_path / "curve.csv"
    temp_file.write_text("1\n2\n3\n")

    curve = CustomCurve(key="test_curve", type="custom", file_path=temp_file)
    assert curve.available() is True

    result = curve.remove()

    assert result is True
    assert curve.file_path is None
    assert not temp_file.exists()


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


def test_custom_curve_from_json_success():
    data = {"key": "abc", "type": "custom"}
    curve = CustomCurve.from_json(data)
    assert curve.key == "abc"
    assert curve.type == "custom"
    # No warnings on success
    assert len(curve.warnings) == 0


def test_custom_curve_from_json_failure_adds_warning():
    """Missing required fields should fall back and add a warning."""
    # Missing both key and type triggers ValidationError path
    curve = CustomCurve.from_json({"unexpected": 123})
    # Fallback returns a constructed model; ensure a warning was recorded
    assert len(curve.warnings) > 0
    base_warnings = curve.warnings.get_by_field("base")
    assert (
        base_warnings and "Failed to create curve from data" in base_warnings[0].message
    )


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


def test_custom_curve_from_dataframe_save_error(tmp_path):
    """Saving data during from_dataframe should warn on failure."""
    import numpy as np

    df = pd.DataFrame({"foo": np.array([1.0, 2.0, 3.0])})

    with (
        patch("pyetm.models.custom_curves.get_settings") as mock_settings,
        patch("pandas.Series.to_csv", side_effect=OSError("disk full")),
    ):
        mock_settings.return_value.path_to_tmp.return_value = tmp_path
        curve = CustomCurve.from_dataframe(df)
        assert isinstance(curve, CustomCurve)
        assert curve.key == "foo"
        # Save failed so file_path not set
        assert curve.file_path is None
        # Warning recorded on curve
        warnings = curve.warnings.get_by_field("foo")
        assert warnings and "Failed to save curve data to file" in warnings[0].message


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


def test_custom_curves_len_iter_and_attachment_helpers():
    """Covers __len__, __iter__, is_attached, attached_keys."""
    c1 = CustomCurve(key="a", type="custom")
    c2 = CustomCurve(key="b", type="custom")
    col = CustomCurves(curves=[c1, c2])

    assert len(col) == 2
    assert [c.key for c in iter(col)] == ["a", "b"]
    assert col.is_attached("a") is True
    assert col.is_attached("z") is False
    assert list(col.attached_keys()) == ["a", "b"]


def test_custom_curves_get_contents_not_found_adds_warning():
    col = CustomCurves(curves=[])
    mock_scenario = Mock()
    res = col.get_contents(mock_scenario, "nope")
    assert res is None
    warnings = col.warnings.get_by_field("curves")
    assert warnings and "not found in collection" in warnings[0].message


def test_custom_curves_get_contents_available_reads_file(tmp_path):
    key = "my_curve"
    data_file = tmp_path / f"{key}.csv"
    data_file.write_text("1\n2\n3\n")
    curve = CustomCurve(key=key, type="custom", file_path=data_file)
    col = CustomCurves(curves=[curve])
    mock_scenario = Mock()

    res = col.get_contents(mock_scenario, key)
    assert isinstance(res, pd.Series)
    assert res.name == key
    # Curve warned about non-8760 values, and warnings merged into collection
    # Warnings are merged with a prefixed field name; just check the message exists
    any_msg = any("Curve length should be 8760" in w.message for w in col.warnings)
    assert any_msg


def test_custom_curves_get_contents_retrieves_when_unavailable(tmp_path):
    key = "remote_curve"
    curve = CustomCurve(key=key, type="custom")
    col = CustomCurves(curves=[curve])
    mock_scenario = Mock()
    mock_scenario.id = 999

    csv_data = io.StringIO("10\n20\n30\n")
    with (
        patch(
            "pyetm.models.custom_curves.DownloadCustomCurveRunner.run",
            return_value=ServiceResult.ok(data=csv_data),
        ),
        patch("pyetm.models.custom_curves.get_settings") as mock_settings,
        patch("pandas.Series.to_csv") as mock_to_csv,
    ):
        mock_settings.return_value.path_to_tmp.return_value = tmp_path / "999"
        res = col.get_contents(mock_scenario, key)
        assert isinstance(res, pd.Series)
        assert res.name == key
        assert curve.file_path is not None


def test_custom_curves_to_dataframe_attempts_retrieve_and_suppresses_errors():
    """Hit branch where retrieve raises but is suppressed when _scenario is set."""
    curve = CustomCurve(key="x", type="custom")
    col = CustomCurves(curves=[curve])
    # Setting the private attr is fine here
    col._scenario = object()
    with patch.object(curve, "retrieve", side_effect=RuntimeError("boom")):
        df = col.to_dataframe()
        # Column exists, index named 'hour'
        assert df.index.name == "hour"
        assert "x" in df.columns


def test_custom_curves_to_dataframe_curve_to_dataframe_raises_adds_warning():
    curve = CustomCurve(key="y", type="custom")
    col = CustomCurves(curves=[curve])
    with patch.object(CustomCurve, "_to_dataframe", side_effect=ValueError("bad")):
        df = col.to_dataframe()
        # Column created as empty series
        assert "y" in df.columns
        warnings = col.warnings.get_by_field("curves")
        assert warnings and "Failed to serialize curve y" in warnings[0].message


def test_custom_curves_from_dataframe_handles_per_column_error():
    import numpy as np

    df = pd.DataFrame({"ok": np.array([1.0, 2.0]), "bad": np.array([3.0, 4.0])})

    def fake_from_df(inner_df, **kwargs):
        name = inner_df.columns[0]
        if name == "bad":
            raise ValueError("oops")
        return CustomCurve(key=name, type="custom")

    with patch.object(CustomCurve, "_from_dataframe", side_effect=fake_from_df):
        col = CustomCurves.from_dataframe(df)
        assert len(col.curves) == 2
        # Warning for the bad column on collection
        # Field names are prefixed; check presence by message
        assert any(
            "Failed to create curve from column bad" in w.message for w in col.warnings
        )


# --- Validate for Upload Tests --- #


def test_validate_for_upload_valid_curves():
    """Test validate_for_upload with valid curves (8760 numeric values)"""
    import numpy as np
    from pathlib import Path
    import shutil

    # Create temporary files with valid data
    temp_dir = Path("/tmp/test_curves")
    temp_dir.mkdir(exist_ok=True)

    try:
        # Valid curve data (8760 values)
        valid_data = np.random.uniform(0, 100, 8760)
        valid_file = temp_dir / "valid_curve.csv"
        pd.Series(valid_data).to_csv(valid_file, header=False, index=False)

        curves = CustomCurves(
            curves=[
                CustomCurve(key="valid_curve", type="profile", file_path=valid_file)
            ]
        )

        validation_errors = curves.validate_for_upload()

        # Should have no errors
        assert len(validation_errors) == 0

    finally:
        # Cleanup - remove entire directory tree
        if temp_dir.exists():
            shutil.rmtree(temp_dir)


def test_validate_for_upload_curve_no_data():
    """Test validate_for_upload with curve that has no data available"""
    curves = CustomCurves(
        curves=[CustomCurve(key="no_data_curve", type="profile")]  # No file_path set
    )

    validation_errors = curves.validate_for_upload()

    assert len(validation_errors) == 1
    assert "no_data_curve" in validation_errors
    warnings_collector = validation_errors["no_data_curve"]
    assert len(warnings_collector) == 1
    warnings_list = list(warnings_collector)
    assert "Curve has no data available" in warnings_list[0].message


def test_validate_for_upload_wrong_length():
    """Test validate_for_upload with curve that has wrong number of values"""
    import numpy as np
    from pathlib import Path
    import shutil

    temp_dir = Path("/tmp/test_curves")
    temp_dir.mkdir(exist_ok=True)

    try:
        # Wrong length data
        short_data = np.random.uniform(0, 100, 100)
        short_file = temp_dir / "short_curve.csv"
        pd.Series(short_data).to_csv(short_file, header=False, index=False)

        curves = CustomCurves(
            curves=[
                CustomCurve(key="short_curve", type="profile", file_path=short_file)
            ]
        )

        validation_errors = curves.validate_for_upload()

        assert len(validation_errors) == 1
        assert "short_curve" in validation_errors
        warnings_collector = validation_errors["short_curve"]
        assert len(warnings_collector) == 1
        warnings_list = list(warnings_collector)
        assert (
            "Curve must contain exactly 8,760 values, found 100"
            in warnings_list[0].message
        )

    finally:
        # Cleanup - remove entire directory tree
        if temp_dir.exists():
            shutil.rmtree(temp_dir)


def test_validate_for_upload_non_numeric_values():
    """Test validate_for_upload with curve that has non-numeric values"""
    from pathlib import Path
    import shutil

    temp_dir = Path("/tmp/test_curves")
    temp_dir.mkdir(exist_ok=True)

    try:
        # Create file with non-numeric data
        non_numeric_file = temp_dir / "non_numeric_curve.csv"
        with open(non_numeric_file, "w") as f:
            # Mix of numeric and non-numeric values
            for i in range(8760):
                if i % 100 == 0:
                    f.write("not_a_number\n")
                else:
                    f.write(f"{i * 0.5}\n")

        curves = CustomCurves(
            curves=[
                CustomCurve(
                    key="non_numeric_curve", type="profile", file_path=non_numeric_file
                )
            ]
        )

        validation_errors = curves.validate_for_upload()

        assert len(validation_errors) == 1
        assert "non_numeric_curve" in validation_errors
        warnings_collector = validation_errors["non_numeric_curve"]
        assert len(warnings_collector) == 1
        warnings_list = list(warnings_collector)
        assert "Curve contains non-numeric values" in warnings_list[0].message

    finally:
        # Cleanup - remove entire directory tree
        if temp_dir.exists():
            shutil.rmtree(temp_dir)


def test_validate_for_upload_empty_curve():
    """Test validate_for_upload with curve that has empty data"""
    from pathlib import Path
    import shutil

    temp_dir = Path("/tmp/test_curves")
    temp_dir.mkdir(exist_ok=True)

    try:
        empty_file = temp_dir / "empty_curve.csv"
        empty_file.touch()

        curves = CustomCurves(
            curves=[
                CustomCurve(key="empty_curve", type="profile", file_path=empty_file)
            ]
        )

        validation_errors = curves.validate_for_upload()

        assert len(validation_errors) == 1
        assert "empty_curve" in validation_errors
        warnings_collector = validation_errors["empty_curve"]
        assert len(warnings_collector) == 1
        warnings_list = list(warnings_collector)
        assert "Curve contains no data" in warnings_list[0].message

    finally:
        # Cleanup - remove entire directory tree
        if temp_dir.exists():
            shutil.rmtree(temp_dir)


def test_validate_for_upload_file_read_error():
    """Test validate_for_upload with curve file that cannot be read"""
    from pathlib import Path

    non_existent_file = Path("/tmp/non_existent_curve.csv")

    curves = CustomCurves(
        curves=[
            CustomCurve(
                key="unreadable_curve", type="profile", file_path=non_existent_file
            )
        ]
    )

    validation_errors = curves.validate_for_upload()

    assert len(validation_errors) == 1
    assert "unreadable_curve" in validation_errors
    warnings_collector = validation_errors["unreadable_curve"]
    assert len(warnings_collector) == 1
    warnings_list = list(warnings_collector)  # Convert to list to access by index
    assert "Error reading curve data:" in warnings_list[0].message


def test_validate_for_upload_empty_dataframe_branch(tmp_path):
    """Force pd.read_csv to return an empty DataFrame to hit raw_data.empty path."""
    f = tmp_path / "empty_data.csv"
    f.write_text("\n\n")
    curves = CustomCurves(curves=[CustomCurve(key="k", type="profile", file_path=f)])

    def fake_read_csv(path, header=None, index_col=False):
        return pd.DataFrame()

    with patch("pyetm.models.custom_curves.pd.read_csv", side_effect=fake_read_csv):
        validation_errors = curves.validate_for_upload()
        assert "k" in validation_errors
        warnings = list(validation_errors["k"])
        assert warnings and "Curve contains no data" in warnings[0].message


def test_validate_for_upload_outer_except_path(tmp_path):
    """Trigger outer exception handler by failing once inside inner except block."""
    f = tmp_path / "raise_once.csv"
    f.write_text("")
    curves = CustomCurves(curves=[CustomCurve(key="zz", type="profile", file_path=f)])

    # Make read_csv raise EmptyDataError to go into that except branch
    def raise_empty(*args, **kwargs):
        raise pd.errors.EmptyDataError("no data")

    class AddOnceFailCollector:
        def __init__(self):
            self._count = 0
            self._records = []

        def add(self, field, message, severity="warning"):
            if self._count == 0:
                self._count += 1
                raise RuntimeError("collector add failed once")
            self._records.append((field, str(message), severity))

        def __len__(self):
            return len(self._records)

        def __iter__(self):
            class W:
                def __init__(self, field, message, severity):
                    self.field = field
                    self.message = message
                    self.severity = severity

            return (W(f, m, s) for (f, m, s) in self._records)

    with (
        patch("pyetm.models.custom_curves.pd.read_csv", side_effect=raise_empty),
        patch("pyetm.models.custom_curves.WarningCollector", AddOnceFailCollector),
    ):
        errors = curves.validate_for_upload()
        # Should have captured via outer except and still recorded a warning
        assert "zz" in errors
        items = list(errors["zz"])  # iter must work
        assert items and "Error reading curve data:" in items[0].message


def test_validate_for_upload_multiple_curves_mixed_validity():
    """Test validate_for_upload with mix of valid and invalid curves"""
    import numpy as np
    from pathlib import Path
    import shutil

    temp_dir = Path("/tmp/test_curves")
    temp_dir.mkdir(exist_ok=True)

    try:
        # Valid curve
        valid_data = np.random.uniform(0, 100, 8760)
        valid_file = temp_dir / "valid_curve.csv"
        pd.Series(valid_data).to_csv(valid_file, header=False, index=False)

        # Invalid curve (wrong length)
        invalid_data = np.random.uniform(0, 100, 100)
        invalid_file = temp_dir / "invalid_curve.csv"
        pd.Series(invalid_data).to_csv(invalid_file, header=False, index=False)

        curves = CustomCurves(
            curves=[
                CustomCurve(key="valid_curve", type="profile", file_path=valid_file),
                CustomCurve(
                    key="invalid_curve", type="profile", file_path=invalid_file
                ),
                CustomCurve(key="no_data_curve", type="profile"),  # No file path
            ]
        )

        validation_errors = curves.validate_for_upload()

        # Should have errors for 2 curves, but not the valid one
        assert len(validation_errors) == 2
        assert "valid_curve" not in validation_errors
        assert "invalid_curve" in validation_errors
        assert "no_data_curve" in validation_errors

        # Check specific error messages
        invalid_warnings = list(validation_errors["invalid_curve"])
        no_data_warnings = list(validation_errors["no_data_curve"])
        assert (
            "Curve must contain exactly 8,760 values, found 100"
            in invalid_warnings[0].message
        )
        assert "Curve has no data available" in no_data_warnings[0].message

    finally:
        # Cleanup - remove entire directory tree
        if temp_dir.exists():
            shutil.rmtree(temp_dir)
