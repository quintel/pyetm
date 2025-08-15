import pandas as pd
from unittest.mock import Mock
import tempfile
import os

from pyetm.models.packables.output_curves_pack import OutputCurvesPack


def make_scenario(id_val="S"):
    s = Mock()
    s.identifier = Mock(return_value=str(id_val))
    s.id = id_val
    return s


def test_to_dataframe_collects_series():
    s = make_scenario()
    s.all_output_curves.return_value = [
        pd.Series([1, 2], name="c1"),
        pd.Series([3, 4], name="c2"),
    ]

    pack = OutputCurvesPack()
    pack.add(s)

    df = pack.to_dataframe()
    assert not df.empty
    assert "c1" in df.columns.get_level_values(1) or "c1" in df.columns


def test_to_dataframe_handles_exception_and_empty(caplog):
    s = make_scenario()
    s.all_output_curves.side_effect = RuntimeError("fail")

    pack = OutputCurvesPack()
    pack.add(s)

    with caplog.at_level("WARNING"):
        df = pack.to_dataframe()
        assert df.empty
        assert "Failed extracting output curves" in caplog.text

    s.all_output_curves.side_effect = None
    s.all_output_curves.return_value = []
    df2 = pack.to_dataframe()
    assert df2.empty


def test_build_dataframe_with_warnings(caplog):
    """Test the warning logging branch when scenario has _output_curves."""
    s = make_scenario()
    s.all_output_curves.return_value = [pd.Series([1, 2], name="test")]

    # Mock _output_curves with log_warnings method
    mock_output_curves = Mock()
    mock_output_curves.log_warnings = Mock()
    s._output_curves = mock_output_curves

    pack = OutputCurvesPack()
    pack.add(s)

    df = pack._build_dataframe_for_scenario(s)

    # Verify log_warnings was called
    mock_output_curves.log_warnings.assert_called_once()
    assert not df.empty


def test_build_dataframe_warning_logging_exception():
    """Test exception handling in warning logging branch."""
    s = make_scenario()
    s.all_output_curves.return_value = [pd.Series([1, 2], name="test")]

    # Mock _output_curves that raises exception during log_warnings
    mock_output_curves = Mock()
    mock_output_curves.log_warnings.side_effect = Exception("logging failed")
    s._output_curves = mock_output_curves

    pack = OutputCurvesPack()
    df = pack._build_dataframe_for_scenario(s)

    # Should still return dataframe despite logging exception
    assert not df.empty


def test_build_dataframe_no_output_curves_attr():
    """Test scenario without _output_curves attribute."""
    s = make_scenario()
    s.all_output_curves.return_value = [pd.Series([1, 2], name="test")]
    # Don't set _output_curves attribute

    pack = OutputCurvesPack()
    df = pack._build_dataframe_for_scenario(s)

    assert not df.empty


def test_build_dataframe_output_curves_none():
    """Test scenario with _output_curves = None."""
    s = make_scenario()
    s.all_output_curves.return_value = [pd.Series([1, 2], name="test")]
    s._output_curves = None

    pack = OutputCurvesPack()
    df = pack._build_dataframe_for_scenario(s)

    assert not df.empty


def test_to_excel_per_carrier_no_scenarios(carrier_mappings):
    """Test to_excel_per_carrier with no scenarios."""

    pack = OutputCurvesPack()
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
    s1.get_output_curves = Mock(
        return_value={
            "demand": pd.Series([100, 200, 300], name="hourly_demand"),
            "supply": pd.DataFrame({"wind": [50, 60, 70], "solar": [30, 40, 50]}),
        }
    )

    s2 = make_scenario("scenario2")
    s2.get_output_curves = Mock(
        return_value={"demand": pd.Series([150, 250, 350], name="hourly_demand")}
    )

    pack = OutputCurvesPack()
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
    s.get_output_curves = Mock(return_value={"demand": pd.Series([1, 2, 3])})

    pack = OutputCurvesPack()
    pack.add(s)

    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
        # Pass invalid carriers - should fall back to valid ones
        pack.to_excel_per_carrier(tmp.name, carriers=["invalid_carrier"])
        os.unlink(tmp.name)


def test_to_excel_scenario_without_get_output_curves(mock_workbook, carrier_mappings):
    """Test scenario without get_output_curves method."""
    mock_wb = mock_workbook["instance"]

    s = make_scenario()
    # Don't add get_output_curves method

    pack = OutputCurvesPack()
    pack.add(s)

    with tempfile.NamedTemporaryFile(suffix=".xlsx") as tmp:
        pack.to_excel_per_carrier(tmp.name)
        # Should not create workbook since no valid curves
    mock_workbook["cls"].assert_not_called()


def test_to_excel_get_output_curves_exception(mock_workbook, carrier_mappings):
    """Test exception in get_output_curves method."""

    s = make_scenario()
    s.get_output_curves = Mock(side_effect=Exception("curves failed"))

    pack = OutputCurvesPack()
    pack.add(s)

    with tempfile.NamedTemporaryFile(suffix=".xlsx") as tmp:
        pack.to_excel_per_carrier(tmp.name)
        # Should handle exception gracefully
    mock_workbook["cls"].assert_not_called()


def test_to_excel_empty_curves_dict(mock_workbook, carrier_mappings):
    """Test scenario with empty curves dictionary."""
    s = make_scenario()
    s.get_output_curves = Mock(return_value={})

    pack = OutputCurvesPack()
    pack.add(s)

    with tempfile.NamedTemporaryFile(suffix=".xlsx") as tmp:
        pack.to_excel_per_carrier(tmp.name)
    mock_workbook["cls"].assert_not_called()


def test_to_excel_none_curves(mock_workbook, carrier_mappings):
    """Test scenario returning None for curves."""
    s = make_scenario()
    s.get_output_curves = Mock(return_value=None)

    pack = OutputCurvesPack()
    pack.add(s)

    with tempfile.NamedTemporaryFile(suffix=".xlsx") as tmp:
        pack.to_excel_per_carrier(tmp.name)
    mock_workbook["cls"].assert_not_called()


def test_to_excel_none_dataframe_values(mock_workbook, carrier_mappings):
    """Test scenario with None values in curves dictionary."""
    s = make_scenario()
    s.get_output_curves = Mock(return_value={"demand": None, "supply": None})

    pack = OutputCurvesPack()
    pack.add(s)

    with tempfile.NamedTemporaryFile(suffix=".xlsx") as tmp:
        pack.to_excel_per_carrier(tmp.name)
    mock_workbook["cls"].assert_not_called()


def test_to_excel_empty_dataframe(mock_workbook, carrier_mappings):
    """Test scenario with empty DataFrame."""
    s = make_scenario()
    empty_df = pd.DataFrame()  # Empty DataFrame
    s.get_output_curves = Mock(return_value={"demand": empty_df})

    pack = OutputCurvesPack()
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
    s.get_output_curves = Mock(return_value={"supply": multi_df})

    pack = OutputCurvesPack()
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
    s.get_output_curves = Mock(return_value={"hourly": single_df})

    pack = OutputCurvesPack()
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

    s.get_output_curves = Mock(return_value={"bad_data": bad_df})

    pack = OutputCurvesPack()
    pack.add(s)

    with tempfile.NamedTemporaryFile(suffix=".xlsx") as tmp:
        pack.to_excel_per_carrier(tmp.name)
        # Should handle exception and not create workbook
    mock_workbook["cls"].assert_not_called()


def test_to_excel_scenario_identifier_exception(mock_workbook, carrier_mappings):
    """Test scenario where identifier() raises exception."""
    s = make_scenario()
    s.identifier.side_effect = Exception("identifier failed")
    s.get_output_curves = Mock(return_value={"demand": pd.Series([1, 2, 3])})

    pack = OutputCurvesPack()
    pack.add(s)

    with tempfile.NamedTemporaryFile(suffix=".xlsx") as tmp:
        pack.to_excel_per_carrier(tmp.name)
        # Should use fallback naming and still work
        mock_workbook["cls"].assert_called_once()


def test_to_excel_empty_carrier_selection(carrier_mappings):
    """Test when carrier selection results in empty list."""
    s = make_scenario()
    s.get_output_curves = Mock(return_value={"demand": pd.Series([1, 2, 3])})

    pack = OutputCurvesPack()
    pack.add(s)

    with tempfile.NamedTemporaryFile(suffix=".xlsx") as tmp:
        pack.to_excel_per_carrier(tmp.name, carriers=["nonexistent1", "nonexistent2"])


def test_class_variables():
    """Test class variables are set correctly."""
    assert OutputCurvesPack.key == "output_curves"
    assert OutputCurvesPack.sheet_name == "OUTPUT_CURVES"


def test_to_dataframe_with_kwargs():
    """Test _to_dataframe passes kwargs correctly by checking it calls the base implementation."""
    s = make_scenario()
    s.all_output_curves.return_value = [pd.Series([1, 2], name="test")]

    pack = OutputCurvesPack()
    pack.add(s)

    # Test that _to_dataframe works with additional kwargs
    df = pack._to_dataframe(columns="test", extra_param="value")
    assert isinstance(df, pd.DataFrame)


def test_build_dataframe_with_columns_kwargs():
    """Test _build_dataframe_for_scenario with columns parameter."""
    s = make_scenario()
    s.all_output_curves.return_value = [pd.Series([1, 2], name="test")]

    pack = OutputCurvesPack()
    df = pack._build_dataframe_for_scenario(s, columns="test_columns", extra="param")

    assert not df.empty
