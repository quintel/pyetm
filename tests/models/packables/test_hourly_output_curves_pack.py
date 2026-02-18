import pandas as pd
from unittest.mock import Mock
import tempfile
import os

from pyetm.models.packables.hourly_output_curves_pack import HourlyOutputCurvesPack


def make_scenario(id_val="S"):
    s = Mock()
    s.identifier = Mock(return_value=str(id_val))
    s.id = id_val
    return s


def _attach_hourly_output_curves(scenario, curve_dict: dict):
    """
    Wire up scenario.hourly_output_curves.attached_keys() and scenario.output_curve()
    to return data from curve_dict, keyed by curve name.
    """
    scenario.hourly_output_curves = Mock()
    scenario.hourly_output_curves.attached_keys = Mock(
        return_value=list(curve_dict.keys())
    )
    scenario.output_curve = Mock(side_effect=lambda name: curve_dict.get(name))


def test_to_dataframe_collects_series():
    s = make_scenario()
    _attach_hourly_output_curves(
        s,
        {
            "merit_order": pd.DataFrame({"wind": [1, 2], "solar": [3, 4]}),
            "electricity_price": pd.DataFrame({"value": [10, 20]}),
        },
    )

    pack = HourlyOutputCurvesPack()
    pack.add(s)

    df = pack.to_dataframe()
    assert not df.empty
    # Level 0 is the scenario identifier; level 1 is curve_type
    assert "merit_order" in df.columns.get_level_values(1)
    assert "electricity_price" in df.columns.get_level_values(1)


def test_to_dataframe_handles_exception_and_empty(caplog):
    s = make_scenario()
    # Force the output_curves accessor to raise
    s.hourly_output_curves = Mock()
    s.hourly_output_curves.attached_keys = Mock(side_effect=RuntimeError("fail"))

    pack = HourlyOutputCurvesPack()
    pack.add(s)

    with caplog.at_level("WARNING"):
        df = pack.to_dataframe()
        assert df.empty
        assert "Failed extracting hourly output curves" in caplog.text

    # Now wire up an empty set of curves
    _attach_hourly_output_curves(s, {})
    df2 = pack.to_dataframe()
    assert df2.empty


def test_build_dataframe_with_warnings(caplog):
    """Test the warning logging branch when scenario has _hourly_output_curves."""
    s = make_scenario()
    _attach_hourly_output_curves(s, {"merit_order": pd.DataFrame({"wind": [1, 2]})})

    # Mock _hourly_output_curves with log_warnings method
    mock_hourly_output_curves = Mock()
    mock_hourly_output_curves.log_warnings = Mock()
    s._hourly_output_curves = mock_hourly_output_curves

    pack = HourlyOutputCurvesPack()
    pack.add(s)

    df = pack._build_dataframe_for_scenario(s)

    # Verify log_warnings was called
    mock_hourly_output_curves.log_warnings.assert_called_once()
    assert not df.empty


def test_build_dataframe_warning_logging_exception():
    """Test exception handling in warning logging branch."""
    s = make_scenario()
    _attach_hourly_output_curves(s, {"merit_order": pd.DataFrame({"wind": [1, 2]})})

    # Mock _hourly_output_curves that raises exception during log_warnings
    mock_hourly_output_curves = Mock()
    mock_hourly_output_curves.log_warnings.side_effect = Exception("logging failed")
    s._hourly_output_curves = mock_hourly_output_curves

    pack = HourlyOutputCurvesPack()
    df = pack._build_dataframe_for_scenario(s)

    # Should still return dataframe despite logging exception
    assert not df.empty


def test_build_dataframe_no_hourly_output_curves_attr():
    """Test scenario without _hourly_output_curves attribute."""
    s = make_scenario()
    _attach_hourly_output_curves(s, {"merit_order": pd.DataFrame({"wind": [1, 2]})})
    # Don't set _hourly_output_curves attribute

    pack = HourlyOutputCurvesPack()
    df = pack._build_dataframe_for_scenario(s)

    assert not df.empty


def test_build_dataframe_hourly_output_curves_none():
    """Test scenario with _hourly_output_curves = None."""
    s = make_scenario()
    _attach_hourly_output_curves(s, {"merit_order": pd.DataFrame({"wind": [1, 2]})})
    s._hourly_output_curves = None

    pack = HourlyOutputCurvesPack()
    df = pack._build_dataframe_for_scenario(s)

    assert not df.empty


def test_build_dataframe_curve_type_multiindex():
    """Verify the (curve_type, column) MultiIndex structure on the result."""
    s = make_scenario()
    _attach_hourly_output_curves(
        s,
        {
            "merit_order": pd.DataFrame({"wind": [10, 20], "solar": [5, 15]}),
            "electricity_price": pd.DataFrame({"value": [100, 200]}),
        },
    )

    pack = HourlyOutputCurvesPack()
    df = pack._build_dataframe_for_scenario(s)

    # Two-level column MultiIndex: (curve_type, original_column)
    assert df.columns.nlevels == 2
    assert df.columns.names[0] == "curve_type"

    # Can slice by curve_type
    merit = df["merit_order"]
    assert list(merit.columns) == ["wind", "solar"]
    assert list(merit["wind"]) == [10, 20]

    price = df["electricity_price"]
    assert list(price.columns) == ["value"]
    assert list(price["value"]) == [100, 200]


def test_build_dataframe_skips_none_and_empty_curves():
    """Curves that return None or empty DataFrames are excluded."""
    s = make_scenario()
    _attach_hourly_output_curves(
        s,
        {
            "merit_order": pd.DataFrame({"wind": [1]}),
            "empty_curve": pd.DataFrame(),  # empty
            "none_curve": None,  # None
        },
    )

    pack = HourlyOutputCurvesPack()
    df = pack._build_dataframe_for_scenario(s)

    assert df.columns.get_level_values(0).tolist() == ["merit_order"]


def test_to_excel_per_carrier_no_scenarios(carrier_mappings):
    """Test to_excel_per_carrier with no scenarios."""

    pack = HourlyOutputCurvesPack()
    # Don't add any scenarios

    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
        pack.to_excel_per_carrier(tmp.name)
        # Should return early, file shouldn't be created with content
        assert os.path.getsize(tmp.name) == 0
        os.unlink(tmp.name)


def test_to_excel_per_carrier_full_flow(
    carrier_mappings, mock_workbook, patch_add_frame
):
    """Test full flow of to_excel_per_carrier."""
    # Setup mocks
    mock_wb = mock_workbook["instance"]

    # Create scenario with output curves
    s1 = make_scenario("scenario1")
    s1.get_hourly_output_curves = Mock(
        return_value={
            "demand": pd.Series([100, 200, 300], name="hourly_demand"),
            "supply": pd.DataFrame({"wind": [50, 60, 70], "solar": [30, 40, 50]}),
        }
    )

    s2 = make_scenario("scenario2")
    s2.get_hourly_output_curves = Mock(
        return_value={"demand": pd.Series([150, 250, 350], name="hourly_demand")}
    )

    pack = HourlyOutputCurvesPack()
    pack.add(s1)
    pack.add(s2)

    with tempfile.NamedTemporaryFile(suffix=".xlsx") as tmp:
        pack.to_excel_per_carrier(tmp.name, carriers=["electricity"])

        # Verify workbook creation and closing
    mock_workbook["cls"].assert_called_once_with(str(tmp.name))
    mock_wb.close.assert_called_once()

    assert patch_add_frame.call_count >= 1


def test_to_excel_invalid_carriers(carrier_mappings):
    """Test to_excel_per_carrier with invalid carriers."""

    s = make_scenario()
    s.get_hourly_output_curves = Mock(return_value={"demand": pd.Series([1, 2, 3])})

    pack = HourlyOutputCurvesPack()
    pack.add(s)

    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
        # Pass invalid carriers - should fall back to valid ones
        pack.to_excel_per_carrier(tmp.name, carriers=["invalid_carrier"])
        os.unlink(tmp.name)


def test_to_excel_scenario_without_get_hourly_output_curves(
    mock_workbook, carrier_mappings
):
    """Test scenario without get_hourly_output_curves method."""
    mock_wb = mock_workbook["instance"]

    s = make_scenario()
    # Don't add get_hourly_output_curves method

    pack = HourlyOutputCurvesPack()
    pack.add(s)

    with tempfile.NamedTemporaryFile(suffix=".xlsx") as tmp:
        pack.to_excel_per_carrier(tmp.name)
        # Should not create workbook since no valid curves
    mock_workbook["cls"].assert_not_called()


def test_to_excel_get_hourly_output_curves_exception(mock_workbook, carrier_mappings):
    """Test exception in get_hourly_output_curves method."""

    s = make_scenario()
    s.get_hourly_output_curves = Mock(side_effect=Exception("curves failed"))

    pack = HourlyOutputCurvesPack()
    pack.add(s)

    with tempfile.NamedTemporaryFile(suffix=".xlsx") as tmp:
        pack.to_excel_per_carrier(tmp.name)
        # Should handle exception gracefully
    mock_workbook["cls"].assert_not_called()


def test_to_excel_empty_curves_dict(mock_workbook, carrier_mappings):
    """Test scenario with empty curves dictionary."""
    s = make_scenario()
    s.get_hourly_output_curves = Mock(return_value={})

    pack = HourlyOutputCurvesPack()
    pack.add(s)

    with tempfile.NamedTemporaryFile(suffix=".xlsx") as tmp:
        pack.to_excel_per_carrier(tmp.name)
    mock_workbook["cls"].assert_not_called()


def test_to_excel_none_curves(mock_workbook, carrier_mappings):
    """Test scenario returning None for curves."""
    s = make_scenario()
    s.get_hourly_output_curves = Mock(return_value=None)

    pack = HourlyOutputCurvesPack()
    pack.add(s)

    with tempfile.NamedTemporaryFile(suffix=".xlsx") as tmp:
        pack.to_excel_per_carrier(tmp.name)
    mock_workbook["cls"].assert_not_called()


def test_to_excel_none_dataframe_values(mock_workbook, carrier_mappings):
    """Test scenario with None values in curves dictionary."""
    s = make_scenario()
    s.get_hourly_output_curves = Mock(return_value={"demand": None, "supply": None})

    pack = HourlyOutputCurvesPack()
    pack.add(s)

    with tempfile.NamedTemporaryFile(suffix=".xlsx") as tmp:
        pack.to_excel_per_carrier(tmp.name)
    mock_workbook["cls"].assert_not_called()


def test_to_excel_empty_dataframe(mock_workbook, carrier_mappings):
    """Test scenario with empty DataFrame."""
    s = make_scenario()
    empty_df = pd.DataFrame()  # Empty DataFrame
    s.get_hourly_output_curves = Mock(return_value={"demand": empty_df})

    pack = HourlyOutputCurvesPack()
    pack.add(s)

    with tempfile.NamedTemporaryFile(suffix=".xlsx") as tmp:
        pack.to_excel_per_carrier(tmp.name)
        # Should not create workbook due to empty DataFrame
    mock_workbook["cls"].assert_not_called()


def test_to_excel_multi_column_dataframe(
    mock_workbook, carrier_mappings, patch_add_frame
):
    """Test scenario with multi-column DataFrame."""
    mock_wb = mock_workbook["instance"]

    s = make_scenario()
    multi_df = pd.DataFrame(
        {"wind": [10, 20, 30], "solar": [5, 15, 25], "hydro": [2, 4, 6]}
    )
    s.get_hourly_output_curves = Mock(return_value={"supply": multi_df})

    pack = HourlyOutputCurvesPack()
    pack.add(s)

    with tempfile.NamedTemporaryFile(suffix=".xlsx") as tmp:
        pack.to_excel_per_carrier(tmp.name)
    mock_workbook["cls"].assert_called_once()
    mock_wb.close.assert_called_once()


def test_to_excel_single_column_dataframe(
    mock_workbook, carrier_mappings, patch_add_frame
):
    """Test scenario with single-column DataFrame."""
    mock_wb = mock_workbook["instance"]

    s = make_scenario()
    single_df = pd.DataFrame({"demand": [100, 200, 300]})
    s.get_hourly_output_curves = Mock(return_value={"hourly": single_df})

    pack = HourlyOutputCurvesPack()
    pack.add(s)

    with tempfile.NamedTemporaryFile(suffix=".xlsx") as tmp:
        pack.to_excel_per_carrier(tmp.name)
    mock_workbook["cls"].assert_called_once()
    mock_wb.close.assert_called_once()


def test_to_excel_dataframe_processing_exception(mock_workbook, carrier_mappings):
    """Test exception during DataFrame processing."""
    s = make_scenario()
    # Create a DataFrame that will cause an exception during processing
    bad_df = Mock(spec=pd.DataFrame)
    bad_df.empty = False
    bad_df.shape = (10, 1)
    bad_df.iloc = Mock()
    bad_df.iloc.__getitem__ = Mock(side_effect=Exception("processing failed"))

    s.get_hourly_output_curves = Mock(return_value={"bad_data": bad_df})

    pack = HourlyOutputCurvesPack()
    pack.add(s)

    with tempfile.NamedTemporaryFile(suffix=".xlsx") as tmp:
        pack.to_excel_per_carrier(tmp.name)
        # Should handle exception and not create workbook
    mock_workbook["cls"].assert_not_called()


def test_to_excel_scenario_identifier_exception(mock_workbook, carrier_mappings):
    """Test scenario where identifier() raises exception."""
    s = make_scenario()
    s.identifier.side_effect = Exception("identifier failed")
    s.get_hourly_output_curves = Mock(return_value={"demand": pd.Series([1, 2, 3])})

    pack = HourlyOutputCurvesPack()
    pack.add(s)

    with tempfile.NamedTemporaryFile(suffix=".xlsx") as tmp:
        pack.to_excel_per_carrier(tmp.name)
        # Should use fallback naming and still work
        mock_workbook["cls"].assert_called_once()


def test_to_excel_empty_carrier_selection(carrier_mappings):
    """Test when carrier selection results in empty list."""
    s = make_scenario()
    s.get_hourly_output_curves = Mock(return_value={"demand": pd.Series([1, 2, 3])})

    pack = HourlyOutputCurvesPack()
    pack.add(s)

    with tempfile.NamedTemporaryFile(suffix=".xlsx") as tmp:
        pack.to_excel_per_carrier(tmp.name, carriers=["nonexistent1", "nonexistent2"])


def test_class_variables():
    """Test class variables are set correctly."""
    assert HourlyOutputCurvesPack.key == "hourly_output_curves"
    assert HourlyOutputCurvesPack.sheet_name == "HOURLY_OUTPUT_CURVES"


def test_to_dataframe_with_kwargs():
    """Test _to_dataframe passes kwargs correctly by checking it calls the base implementation."""
    s = make_scenario()
    s.all_hourly_output_curves.return_value = [pd.Series([1, 2], name="test")]

    pack = HourlyOutputCurvesPack()
    pack.add(s)

    # Test that _to_dataframe works with additional kwargs
    df = pack._to_dataframe(columns="test", extra_param="value")
    assert isinstance(df, pd.DataFrame)


def test_build_dataframe_with_columns_kwargs():
    """Test _build_dataframe_for_scenario with columns parameter."""
    s = make_scenario()
    _attach_hourly_output_curves(s, {"merit_order": pd.DataFrame({"wind": [1, 2]})})

    pack = HourlyOutputCurvesPack()
    df = pack._build_dataframe_for_scenario(s, columns="test_columns", extra="param")

    assert not df.empty


def test_to_dataframe_with_curves_filter():
    """Test filtering to specific curves using curves parameter."""
    s = make_scenario()
    _attach_hourly_output_curves(
        s,
        {
            "merit_order": pd.DataFrame({"wind": [1, 2], "solar": [3, 4]}),
            "electricity_price": pd.DataFrame({"value": [10, 20]}),
            "heat_network": pd.DataFrame({"demand": [100, 200]}),
        },
    )

    pack = HourlyOutputCurvesPack()
    pack.add(s)

    # Filter to only merit_order and electricity_price
    df = pack.to_dataframe(curves=["merit_order", "electricity_price"])

    assert not df.empty
    # Should have 2 curve types
    curve_types = df.columns.get_level_values(1).unique().tolist()
    assert "merit_order" in curve_types
    assert "electricity_price" in curve_types
    assert "heat_network" not in curve_types


def test_to_dataframe_with_invalid_curves_filter():
    """Test filtering with curve names that don't exist."""
    s = make_scenario()
    _attach_hourly_output_curves(
        s,
        {
            "merit_order": pd.DataFrame({"wind": [1, 2]}),
        },
    )

    pack = HourlyOutputCurvesPack()
    pack.add(s)

    # Request curves that don't exist
    df = pack.to_dataframe(curves=["nonexistent1", "nonexistent2"])

    assert df.empty
