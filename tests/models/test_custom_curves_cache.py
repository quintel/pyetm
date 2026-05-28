"""Tests for custom curves cache clearing functionality."""

from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from pyetm.models.custom_curves import CustomCurve, CustomCurves


@pytest.fixture
def custom_curve_with_file(tmp_path):
    """Create a CustomCurve with an actual cached file."""
    csv_path = tmp_path / "weather-insulation.csv"
    df = pd.DataFrame({"hour": range(8760), "value": range(8760)})
    df.to_csv(csv_path, index=False, header=False)

    curve = CustomCurve(key="weather/insulation")
    curve.file_path = csv_path
    return curve


@pytest.fixture
def custom_curves_collection(tmp_path):
    """Create a CustomCurves collection with cached files."""
    curves = []

    # Create 3 curves with cached files
    for curve_name in ["weather/insulation", "weather/buildings", "weather/solar"]:
        csv_path = tmp_path / f"{curve_name.replace('/', '-')}.csv"
        df = pd.DataFrame({"hour": range(8760), "value": range(8760)})
        df.to_csv(csv_path, index=False, header=False)

        curve = CustomCurve(key=curve_name)
        curve.file_path = csv_path
        curves.append(curve)

    return CustomCurves(curves=curves)


class TestCustomCurveRemove:
    """Tests for CustomCurve.remove() method."""

    def test_remove_existing_file(self, custom_curve_with_file):
        """Test removing an existing curve file."""
        file_path = custom_curve_with_file.file_path
        assert file_path.exists()

        result = custom_curve_with_file.remove()

        assert result is True
        assert not file_path.exists()
        assert custom_curve_with_file.file_path is None

    def test_remove_nonexistent_file(self):
        """Test removing when file doesn't exist returns True."""
        curve = CustomCurve(key="test")
        curve.file_path = None

        result = curve.remove()

        assert result is True

    def test_remove_handles_errors(self):
        """Test that remove() handles file deletion errors gracefully."""
        # Test with an invalid path that doesn't exist but is set
        curve = CustomCurve(key="test")
        # Set a path that doesn't exist
        curve.file_path = Path("/nonexistent/directory/test.csv")

        # available() will return False, so remove() should return True
        result = curve.remove()
        assert result is True


class TestCustomCurvesRemoveCurve:
    """Tests for CustomCurves.remove_curve() method."""

    def test_remove_curve_by_name(self, custom_curves_collection):
        """Test removing a single curve by name."""
        result = custom_curves_collection.remove_curve("weather/insulation")

        assert result is True
        curve = custom_curves_collection._find("weather/insulation")
        assert curve.file_path is None

    def test_remove_nonexistent_curve(self, custom_curves_collection):
        """Test removing a curve that doesn't exist."""
        result = custom_curves_collection.remove_curve("nonexistent/curve")

        assert result is False

    def test_remove_curve_doesnt_affect_others(self, custom_curves_collection):
        """Test that removing one curve doesn't affect others."""
        buildings_curve = custom_curves_collection._find("weather/buildings")
        buildings_file_path = buildings_curve.file_path

        custom_curves_collection.remove_curve("weather/insulation")

        assert buildings_file_path.exists()
        assert buildings_curve.file_path == buildings_file_path

    def test_remove_curve_with_slash_in_name(self, custom_curves_collection):
        """Test removing a curve with slash in the name."""
        result = custom_curves_collection.remove_curve("weather/solar")

        assert result is True
        curve = custom_curves_collection._find("weather/solar")
        assert curve.file_path is None


class TestCustomCurvesClearCache:
    """Tests for CustomCurves.clear_cache() method."""

    def test_clear_cache_removes_all_files(self, custom_curves_collection):
        """Test that clear_cache() removes all curve files."""
        # Verify files exist
        for curve in custom_curves_collection.curves:
            assert curve.file_path.exists()

        removed_count = custom_curves_collection.clear_cache()

        assert removed_count == 3
        for curve in custom_curves_collection.curves:
            assert curve.file_path is None

    def test_clear_cache_clears_warnings(self, custom_curves_collection):
        """Test that clear_cache() clears collection warnings."""
        custom_curves_collection.add_warning("test_key", "Test warning")
        assert len(custom_curves_collection.warnings) > 0

        custom_curves_collection.clear_cache()

        assert len(custom_curves_collection.warnings) == 0

    def test_clear_cache_with_no_files(self):
        """Test clear_cache() with no cached files."""
        curves = [
            CustomCurve(key="weather/test1"),
            CustomCurve(key="weather/test2"),
        ]
        collection = CustomCurves(curves=curves)

        removed_count = collection.clear_cache()

        # remove() returns True for curves without files, so they're counted
        # This is consistent behavior - no files to remove means successful removal
        assert removed_count == 2

    def test_clear_cache_with_mixed_files(self, tmp_path):
        """Test clear_cache() with mix of existing and non-existing files."""
        curves = []

        # Create 2 curves with actual files
        for i in range(2):
            csv_path = tmp_path / f"exists-{i}.csv"
            df = pd.DataFrame({"value": [i]})
            df.to_csv(csv_path, index=False, header=False)

            curve = CustomCurve(key=f"weather/exists_{i}")
            curve.file_path = csv_path
            curves.append(curve)

        # Create 1 curve with a path that doesn't exist
        missing_path = tmp_path / "missing.csv"
        curve_missing = CustomCurve(key="weather/missing")
        curve_missing.file_path = missing_path
        curves.append(curve_missing)

        collection = CustomCurves(curves=curves)

        removed_count = collection.clear_cache()

        # All 3 should succeed (missing file unlink uses missing_ok=True)
        assert removed_count == 3

    def test_clear_cache_returns_correct_count(self, tmp_path):
        """Test that clear_cache() returns accurate count of removed files."""
        curves = []

        # Create 5 curves with files
        for i in range(5):
            csv_path = tmp_path / f"curve_{i}.csv"
            df = pd.DataFrame({"value": [i]})
            df.to_csv(csv_path, index=False, header=False)

            curve = CustomCurve(key=f"weather/curve_{i}")
            curve.file_path = csv_path
            curves.append(curve)

        collection = CustomCurves(curves=curves)

        removed_count = collection.clear_cache()

        assert removed_count == 5
        for curve in collection.curves:
            assert curve.file_path is None

    def test_clear_cache_with_mixed_states(self, tmp_path):
        """Test clear_cache() with mix of cached and non-cached curves."""
        curves = []

        # Create 2 curves with files
        for i in range(2):
            csv_path = tmp_path / f"cached_{i}.csv"
            df = pd.DataFrame({"value": [i]})
            df.to_csv(csv_path, index=False, header=False)

            curve = CustomCurve(key=f"weather/cached_{i}")
            curve.file_path = csv_path
            curves.append(curve)

        # Create 2 curves without files
        for i in range(2):
            curve = CustomCurve(key=f"weather/notcached_{i}")
            curves.append(curve)

        collection = CustomCurves(curves=curves)

        removed_count = collection.clear_cache()

        # All 4 counted (2 actual file removals + 2 successful no-ops)
        assert removed_count == 4
