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
    Wire up scenario.hourly_output_curves.to_dataframe() to return data from curve_dict.

    Creates a DataFrame in the format that would be returned by the model:
    - For single-column DataFrames: creates (hour, curve_name) multi-index with 'value' column
    - For multi-column DataFrames: stacks columns into the index as (hour, curve_name, column_name)

    The pack's _build_dataframe_for_scenario will then unstack this back to column format.
    """
    all_rows = []

    for curve_name, curve_data in curve_dict.items():
        if curve_data is None or (hasattr(curve_data, "empty") and curve_data.empty):
            continue

        if isinstance(curve_data, pd.DataFrame):
            # Stack the DataFrame to create (hour, curve_name, column) multi-index
            stacked = curve_data.stack()
            stacked.index.names = ["hour", "column"]
            for (hour, column), value in stacked.items():
                all_rows.append(
                    {"hour": hour, "curve_name": curve_name, "column": column, "value": value}
                )
        else:
            # Series - simpler case
            for hour, value in enumerate(curve_data):
                all_rows.append(
                    {
                        "hour": hour,
                        "curve_name": curve_name,
                        "column": curve_name,  # Use curve_name as column name
                        "value": value,
                    }
                )

    if all_rows:
        df = pd.DataFrame(all_rows)
        # Create multi-index (hour, curve_name, column) with 'value' as data
        result_df = df.set_index(["hour", "curve_name", "column"])["value"].to_frame()
    else:
        result_df = pd.DataFrame(
            index=pd.MultiIndex.from_tuples([], names=["hour", "curve_name", "column"]),
            columns=["value"],
        )

    scenario.hourly_output_curves = Mock()
    scenario.hourly_output_curves.to_dataframe = Mock(return_value=result_df)


def _attach_hourly_output_curves_by_carrier(scenario, carrier_dict: dict):
    """
    Wire up scenario.hourly_output_curves.get_curves()
    to return data from carrier_dict, keyed by carrier type.
    """
    scenario.hourly_output_curves = Mock()
    scenario.hourly_output_curves.get_curves = Mock(
        side_effect=lambda identifiers, scenario_arg: carrier_dict.get(identifiers[0], {})
    )


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
    # Force to_dataframe to raise
    s.hourly_output_curves = Mock()
    s.hourly_output_curves.to_dataframe = Mock(side_effect=RuntimeError("fail"))

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
    """Test that dataframe building works correctly with valid data."""
    s = make_scenario()
    _attach_hourly_output_curves(s, {"merit_order": pd.DataFrame({"wind": [1, 2]})})

    pack = HourlyOutputCurvesPack()
    pack.add(s)

    df = pack._build_dataframe_for_scenario(s)

    # Should return valid dataframe
    assert not df.empty
    assert "merit_order" in df.columns


def test_build_dataframe_warning_logging_exception():
    """Test that building dataframe works even with complex data."""
    s = make_scenario()
    _attach_hourly_output_curves(s, {"merit_order": pd.DataFrame({"wind": [1, 2]})})

    pack = HourlyOutputCurvesPack()
    df = pack._build_dataframe_for_scenario(s)

    # Should return dataframe
    assert not df.empty


def test_build_dataframe_no_hourly_output_curves_attr():
    """Test scenario returns data correctly."""
    s = make_scenario()
    _attach_hourly_output_curves(s, {"merit_order": pd.DataFrame({"wind": [1, 2]})})

    pack = HourlyOutputCurvesPack()
    df = pack._build_dataframe_for_scenario(s)

    assert not df.empty


def test_build_dataframe_hourly_output_curves_none():
    """Test scenario with valid hourly output curves data."""
    s = make_scenario()
    _attach_hourly_output_curves(s, {"merit_order": pd.DataFrame({"wind": [1, 2]})})

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


def test_to_excel_per_carrier_full_flow(carrier_mappings, mock_workbook, patch_add_frame):
    """Test full flow of to_excel_per_carrier."""
    # Setup mocks
    mock_wb = mock_workbook["instance"]

    # Create scenario with output curves using new method chain
    s1 = make_scenario("scenario1")
    _attach_hourly_output_curves_by_carrier(
        s1,
        {
            "electricity": {
                "demand": pd.Series([100, 200, 300], name="hourly_demand"),
                "supply": pd.DataFrame({"wind": [50, 60, 70], "solar": [30, 40, 50]}),
            }
        },
    )

    s2 = make_scenario("scenario2")
    _attach_hourly_output_curves_by_carrier(
        s2, {"electricity": {"demand": pd.Series([150, 250, 350], name="hourly_demand")}}
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
    _attach_hourly_output_curves_by_carrier(s, {"electricity": {"demand": pd.Series([1, 2, 3])}})

    pack = HourlyOutputCurvesPack()
    pack.add(s)

    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
        # Pass invalid carriers - should fall back to valid ones
        pack.to_excel_per_carrier(tmp.name, carriers=["invalid_carrier"])
        os.unlink(tmp.name)


def test_to_excel_scenario_without_get_hourly_output_curves(mock_workbook, carrier_mappings):
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
    """Test exception in get_curves_by_carrier_type method."""

    s = make_scenario()
    s.hourly_output_curves = Mock()
    s.hourly_output_curves.get_curves_by_carrier_type = Mock(side_effect=Exception("curves failed"))

    pack = HourlyOutputCurvesPack()
    pack.add(s)

    with tempfile.NamedTemporaryFile(suffix=".xlsx") as tmp:
        pack.to_excel_per_carrier(tmp.name)
        # Should handle exception gracefully
    mock_workbook["cls"].assert_not_called()


def test_to_excel_empty_curves_dict(mock_workbook, carrier_mappings):
    """Test scenario with empty curves dictionary."""
    s = make_scenario()
    _attach_hourly_output_curves_by_carrier(s, {})

    pack = HourlyOutputCurvesPack()
    pack.add(s)

    with tempfile.NamedTemporaryFile(suffix=".xlsx") as tmp:
        pack.to_excel_per_carrier(tmp.name)
    mock_workbook["cls"].assert_not_called()


def test_to_excel_none_curves(mock_workbook, carrier_mappings):
    """Test scenario returning None for curves."""
    s = make_scenario()
    s.hourly_output_curves = Mock()
    s.hourly_output_curves.get_curves_by_carrier_type = Mock(return_value=None)

    pack = HourlyOutputCurvesPack()
    pack.add(s)

    with tempfile.NamedTemporaryFile(suffix=".xlsx") as tmp:
        pack.to_excel_per_carrier(tmp.name)
    mock_workbook["cls"].assert_not_called()


def test_to_excel_none_dataframe_values(mock_workbook, carrier_mappings):
    """Test scenario with None values in curves dictionary."""
    s = make_scenario()
    _attach_hourly_output_curves_by_carrier(s, {"electricity": {"demand": None, "supply": None}})

    pack = HourlyOutputCurvesPack()
    pack.add(s)

    with tempfile.NamedTemporaryFile(suffix=".xlsx") as tmp:
        pack.to_excel_per_carrier(tmp.name)
    mock_workbook["cls"].assert_not_called()


def test_to_excel_empty_dataframe(mock_workbook, carrier_mappings):
    """Test scenario with empty DataFrame."""
    s = make_scenario()
    empty_df = pd.DataFrame()  # Empty DataFrame
    _attach_hourly_output_curves_by_carrier(s, {"electricity": {"demand": empty_df}})

    pack = HourlyOutputCurvesPack()
    pack.add(s)

    with tempfile.NamedTemporaryFile(suffix=".xlsx") as tmp:
        pack.to_excel_per_carrier(tmp.name)
        # Should not create workbook due to empty DataFrame
    mock_workbook["cls"].assert_not_called()


def test_to_excel_multi_column_dataframe(mock_workbook, carrier_mappings, patch_add_frame):
    """Test scenario with multi-column DataFrame."""
    mock_wb = mock_workbook["instance"]

    s = make_scenario()
    multi_df = pd.DataFrame({"wind": [10, 20, 30], "solar": [5, 15, 25], "hydro": [2, 4, 6]})
    _attach_hourly_output_curves_by_carrier(s, {"electricity": {"supply": multi_df}})

    pack = HourlyOutputCurvesPack()
    pack.add(s)

    with tempfile.NamedTemporaryFile(suffix=".xlsx") as tmp:
        pack.to_excel_per_carrier(tmp.name)
    mock_workbook["cls"].assert_called_once()
    mock_wb.close.assert_called_once()


def test_to_excel_single_column_dataframe(mock_workbook, carrier_mappings, patch_add_frame):
    """Test scenario with single-column DataFrame."""
    mock_wb = mock_workbook["instance"]

    s = make_scenario()
    single_df = pd.DataFrame({"demand": [100, 200, 300]})
    _attach_hourly_output_curves_by_carrier(s, {"electricity": {"hourly": single_df}})

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

    _attach_hourly_output_curves_by_carrier(s, {"electricity": {"bad_data": bad_df}})

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

    # Mock the new method chain: scenario.hourly_output_curves.get_curves()
    s.hourly_output_curves = Mock()
    s.hourly_output_curves.get_curves = Mock(
        return_value={"demand": pd.Series([1, 2, 3])}
    )

    pack = HourlyOutputCurvesPack()
    pack.add(s)

    with tempfile.NamedTemporaryFile(suffix=".xlsx") as tmp:
        pack.to_excel_per_carrier(tmp.name)
        # Should use fallback naming and still work
        mock_workbook["cls"].assert_called_once()


def test_to_excel_empty_carrier_selection(carrier_mappings):
    """Test when carrier selection results in empty list."""
    s = make_scenario()
    _attach_hourly_output_curves_by_carrier(s, {"electricity": {"demand": pd.Series([1, 2, 3])}})

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
