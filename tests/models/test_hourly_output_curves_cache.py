"""Tests for hourly output curves cache clearing functionality."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from pyetm.models.hourly_output_curves import (
    HourlyOutputCurve,
    HourlyOutputCurves,
    _read_csv_cached_impl,
)


@pytest.fixture
def mock_session():
    """Create a mock session for testing."""
    session = MagicMock()
    session.id = 12345
    session.settings.path_to_tmp.return_value = Path("/tmp/pyetm/12345")
    return session


@pytest.fixture
def hourly_curve_with_file(tmp_path):
    """Create a HourlyOutputCurve with an actual cached file."""
    csv_path = tmp_path / "merit-order.csv"
    df = pd.DataFrame({"hour": range(8760), "value": range(8760)})
    df.to_csv(csv_path)

    curve = HourlyOutputCurve(key="merit_order", type="merit_order")
    curve.file_path = csv_path
    return curve


@pytest.fixture
def hourly_curves_collection(tmp_path):
    """Create a HourlyOutputCurves collection with cached files."""
    curves = []

    # Create 3 curves with cached files
    for curve_name in ["merit_order", "electricity_price", "heat_network"]:
        csv_path = tmp_path / f"{curve_name.replace('_', '-')}.csv"
        df = pd.DataFrame({"hour": range(8760), "value": range(8760)})
        df.to_csv(csv_path)

        curve = HourlyOutputCurve(key=curve_name, type=curve_name)
        curve.file_path = csv_path
        curves.append(curve)

    return HourlyOutputCurves(curves=curves)


class TestHourlyOutputCurveRemove:
    """Tests for HourlyOutputCurve.remove() method."""

    def test_remove_existing_file(self, hourly_curve_with_file):
        """Test removing an existing curve file."""
        file_path = hourly_curve_with_file.file_path
        assert file_path.exists()

        # Clear cache before test
        _read_csv_cached_impl.cache_clear()

        result = hourly_curve_with_file.remove()

        assert result is True
        assert not file_path.exists()
        assert hourly_curve_with_file.file_path is None

    def test_remove_nonexistent_file(self):
        """Test removing when file doesn't exist returns True."""
        curve = HourlyOutputCurve(key="test", type="merit_order")
        curve.file_path = None

        result = curve.remove()

        assert result is True

    def test_remove_clears_lru_cache(self, hourly_curve_with_file):
        """Test that remove() clears the LRU cache."""
        _read_csv_cached_impl.cache_clear()

        # Populate cache by reading
        _ = _read_csv_cached_impl(
            str(hourly_curve_with_file.file_path),
            hourly_curve_with_file.file_path.stat().st_mtime,
        )

        cache_info_before = _read_csv_cached_impl.cache_info()
        assert cache_info_before.currsize > 0

        hourly_curve_with_file.remove()

        cache_info_after = _read_csv_cached_impl.cache_info()
        assert cache_info_after.currsize == 0

    def test_remove_handles_errors(self):
        """Test that remove() handles file deletion errors gracefully."""
        # Test with an invalid path that doesn't exist but is set
        curve = HourlyOutputCurve(key="test", type="merit_order")
        # Set a path that doesn't exist
        curve.file_path = Path("/nonexistent/directory/test.csv")

        # available() will return False, so remove() should return True
        result = curve.remove()
        assert result is True


class TestHourlyOutputCurvesRemoveCurve:
    """Tests for HourlyOutputCurves.remove_curve() method."""

    def test_remove_curve_by_name(self, hourly_curves_collection):
        """Test removing a single curve by name."""
        _read_csv_cached_impl.cache_clear()

        result = hourly_curves_collection.remove_curve("merit_order")

        assert result is True
        curve = hourly_curves_collection._find("merit_order")
        assert curve.file_path is None

    def test_remove_curve_clears_lru_cache(self, hourly_curves_collection):
        """Test that remove_curve() clears the LRU cache."""
        _read_csv_cached_impl.cache_clear()

        # Populate cache
        curve = hourly_curves_collection._find("merit_order")
        _ = _read_csv_cached_impl(str(curve.file_path), curve.file_path.stat().st_mtime)

        cache_info_before = _read_csv_cached_impl.cache_info()
        assert cache_info_before.currsize > 0

        hourly_curves_collection.remove_curve("merit_order")

        cache_info_after = _read_csv_cached_impl.cache_info()
        assert cache_info_after.currsize == 0

    def test_remove_nonexistent_curve(self, hourly_curves_collection):
        """Test removing a curve that doesn't exist."""
        result = hourly_curves_collection.remove_curve("nonexistent_curve")

        assert result is False

    def test_remove_curve_doesnt_affect_others(self, hourly_curves_collection):
        """Test that removing one curve doesn't affect others."""
        heat_curve = hourly_curves_collection._find("heat_network")
        heat_file_path = heat_curve.file_path

        hourly_curves_collection.remove_curve("merit_order")

        assert heat_file_path.exists()
        assert heat_curve.file_path == heat_file_path


class TestHourlyOutputCurvesClearCache:
    """Tests for HourlyOutputCurves.clear_cache() method."""

    def test_clear_cache_removes_all_files(self, hourly_curves_collection):
        """Test that clear_cache() removes all curve files."""
        _read_csv_cached_impl.cache_clear()

        # Verify files exist
        for curve in hourly_curves_collection.curves:
            assert curve.file_path.exists()

        removed_count = hourly_curves_collection.clear_cache()

        assert removed_count == 3
        for curve in hourly_curves_collection.curves:
            assert curve.file_path is None

    def test_clear_cache_clears_lru_cache(self, hourly_curves_collection):
        """Test that clear_cache() clears the LRU cache."""
        _read_csv_cached_impl.cache_clear()

        # Populate cache with all curves
        for curve in hourly_curves_collection.curves:
            _ = _read_csv_cached_impl(
                str(curve.file_path), curve.file_path.stat().st_mtime
            )

        cache_info_before = _read_csv_cached_impl.cache_info()
        assert cache_info_before.currsize == 3

        hourly_curves_collection.clear_cache()

        cache_info_after = _read_csv_cached_impl.cache_info()
        assert cache_info_after.currsize == 0

    def test_clear_cache_clears_warnings(self, hourly_curves_collection):
        """Test that clear_cache() clears collection warnings."""
        hourly_curves_collection.add_warning("test_key", "Test warning")
        assert len(hourly_curves_collection.warnings) > 0

        hourly_curves_collection.clear_cache()

        assert len(hourly_curves_collection.warnings) == 0

    def test_clear_cache_with_no_files(self):
        """Test clear_cache() with no cached files."""
        curves = [
            HourlyOutputCurve(key="merit_order", type="merit_order"),
            HourlyOutputCurve(key="electricity_price", type="electricity_price"),
        ]
        collection = HourlyOutputCurves(curves=curves)

        removed_count = collection.clear_cache()

        # remove() returns True for curves without files, so they're counted
        # This is consistent behavior - no files to remove means successful removal
        assert removed_count == 2

    def test_clear_cache_with_mixed_files(self, tmp_path):
        """Test clear_cache() with mix of existing and non-existing files."""
        curves = []

        # Create 2 curves with actual files
        for i in range(2):
            csv_path = tmp_path / f"exists_{i}.csv"
            df = pd.DataFrame({"value": [i]})
            df.to_csv(csv_path)

            curve = HourlyOutputCurve(key=f"exists_{i}", type="merit_order")
            curve.file_path = csv_path
            curves.append(curve)

        # Create 1 curve with a path that doesn't exist
        missing_path = tmp_path / "missing.csv"
        curve_missing = HourlyOutputCurve(key="missing", type="merit_order")
        curve_missing.file_path = missing_path
        curves.append(curve_missing)

        collection = HourlyOutputCurves(curves=curves)

        removed_count = collection.clear_cache()

        # All 3 should succeed (missing file unlink uses missing_ok=True)
        assert removed_count == 3
        # LRU cache should be cleared
        assert _read_csv_cached_impl.cache_info().currsize == 0

    def test_clear_cache_returns_correct_count(self, tmp_path):
        """Test that clear_cache() returns accurate count of removed files."""
        curves = []

        # Create 5 curves with files
        for i in range(5):
            csv_path = tmp_path / f"curve_{i}.csv"
            df = pd.DataFrame({"value": [i]})
            df.to_csv(csv_path)

            curve = HourlyOutputCurve(key=f"curve_{i}", type="merit_order")
            curve.file_path = csv_path
            curves.append(curve)

        collection = HourlyOutputCurves(curves=curves)

        removed_count = collection.clear_cache()

        assert removed_count == 5
        for curve in collection.curves:
            assert curve.file_path is None
