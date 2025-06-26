import pytest
import pandas as pd

from pathlib import Path
from unittest.mock import MagicMock, patch

from pyetm.models import CustomCurves, Scenario

def set_path(curve):
    curve.file_path = Path('tests/fixtures/interconnector_2_export_availability.csv')

def test_custom_curves_from_json(custom_curves_json):
    custom_curves = CustomCurves.from_json(custom_curves_json)

    # Check if valid!
    assert custom_curves
    assert len(custom_curves) == 3
    assert next(custom_curves.attached_keys()) == "interconnector_2_export_availability"
    assert not next(iter(custom_curves)).available()

def test_get_contents(custom_curves_json, scenario):
    # 1. curve that was not chached
    # 2. curve that was cached
    # 3. curve that does not exsit

    custom_curves = CustomCurves.from_json(custom_curves_json)
    curve = custom_curves._find("interconnector_2_export_availability")

    # Case 1: curve that was not cached
    curve.file_path = None

    # patch the retrieve method to just set the file path to our fixture
    with patch("pyetm.models.custom_curves.CustomCurve.retrieve", new=MagicMock(side_effect=set_path(curve))):
        curve_content = custom_curves.get_contents(scenario, "interconnector_2_export_availability")

        assert isinstance(curve_content, pd.Series)


    # Case 2: curve that was cached
    # Mock cached content with fixture
    set_path(curve)

    curve_content = custom_curves.get_contents(scenario, "interconnector_2_export_availability")

    assert isinstance(curve_content, pd.Series)


    # Case 3: curve that was cached
    curve_content = custom_curves.get_contents(scenario, "non_existent")

    assert not curve_content


