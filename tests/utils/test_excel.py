import pytest
import pandas as pd
import numpy as np
import tempfile
import os
from unittest.mock import Mock, patch, call
from xlsxwriter.workbook import Workbook
from xlsxwriter.worksheet import Worksheet

from pyetm.utils.excel import (
    add_frame,
    handle_numeric_value,
    set_column_widths,
    write_index,
    add_series,
    create_scenario_formats,
    get_scenario_blocks,
)


class TestHandleNumericValue:

    def setup_method(self):
        """Setup mock worksheet for each test"""
        self.mock_worksheet = Mock(spec=Worksheet)

    def test_handle_nan_as_formula(self):
        """Test NaN handling with formula (default)"""
        result = handle_numeric_value(
            self.mock_worksheet, 1, 2, np.nan, None, nan_as_formula=True
        )

        self.mock_worksheet.write_formula.assert_called_once_with(
            1, 2, "=NA()", None, "#N/A"
        )

    def test_handle_nan_as_text(self):
        """Test NaN handling as text"""
        result = handle_numeric_value(
            self.mock_worksheet, 1, 2, np.nan, None, nan_as_formula=False
        )

        self.mock_worksheet.write.assert_called_once_with(1, 2, "N/A", None)

    def test_handle_regular_number(self):
        """Test normal number handling"""
        result = handle_numeric_value(self.mock_worksheet, 1, 2, 3.14159, None)

        # Should write with default precision (10 decimal places)
        expected_value = 3.14159
        self.mock_worksheet.write_number.assert_called_once_with(
            1, 2, expected_value, None
        )

    def test_handle_number_with_precision(self):
        """Test number with custom precision"""
        # Number that needs rounding
        result = handle_numeric_value(
            self.mock_worksheet, 1, 2, 1.123456789012345, None, decimal_precision=5
        )

        # Should round to 5 decimal places
        args = self.mock_worksheet.write_number.call_args[0]
        assert abs(args[2] - 1.12346) < 1e-10

    def test_handle_zero(self):
        """Test zero value"""
        result = handle_numeric_value(self.mock_worksheet, 0, 0, 0.0, None)
        self.mock_worksheet.write_number.assert_called_once_with(0, 0, 0.0, None)

    def test_handle_negative_number(self):
        """Test negative number handling"""
        result = handle_numeric_value(self.mock_worksheet, 0, 0, -5.5, None)
        self.mock_worksheet.write_number.assert_called_once_with(0, 0, -5.5, None)

    def test_handle_number_with_cell_format(self):
        """Test number handling with cell format"""
        mock_format = Mock()
        result = handle_numeric_value(self.mock_worksheet, 1, 1, 42.0, mock_format)
        self.mock_worksheet.write_number.assert_called_once_with(
            1, 1, 42.0, mock_format
        )

    def test_handle_nan_with_cell_format(self):
        """Test NaN handling with cell format"""
        mock_format = Mock()
        result = handle_numeric_value(
            self.mock_worksheet, 1, 1, np.nan, mock_format, nan_as_formula=True
        )
        self.mock_worksheet.write_formula.assert_called_once_with(
            1, 1, "=NA()", mock_format, "#N/A"
        )

    def test_decimal_precision_edge_cases(self):
        """Test decimal precision with edge cases"""
        # Test precision = 0
        result = handle_numeric_value(
            self.mock_worksheet, 0, 0, 3.14159, None, decimal_precision=0
        )
        args = self.mock_worksheet.write_number.call_args[0]
        assert args[2] == 4.0  # Should ceil to 4

        # Test very high precision
        result = handle_numeric_value(
            self.mock_worksheet, 0, 0, 1.23456789, None, decimal_precision=15
        )
        self.mock_worksheet.write_number.assert_called_with(0, 0, 1.23456789, None)

    def test_handle_positive_infinity(self):
        """Test handling positive infinity"""
        # Infinity will cause OverflowError in math.ceil, so it should be handled
        # The function should still try to process it, but the math.ceil will fail
        with pytest.raises(OverflowError):
            handle_numeric_value(self.mock_worksheet, 0, 0, float("inf"), None)

    def test_handle_negative_infinity(self):
        """Test handling negative infinity"""
        # Negative infinity will cause OverflowError in math.ceil
        with pytest.raises(OverflowError):
            handle_numeric_value(self.mock_worksheet, 0, 0, float("-inf"), None)

    def test_handle_very_small_number(self):
        """Test handling very small numbers"""
        very_small = 1e-10  # Use a less extreme small number
        result = handle_numeric_value(
            self.mock_worksheet, 0, 0, very_small, None, decimal_precision=10
        )

        # Should write the small number (may be rounded by precision)
        args = self.mock_worksheet.write_number.call_args[0]
        # Check that it's close to the expected value
        assert abs(args[2] - very_small) < 1e-15

    def test_handle_very_large_number(self):
        """Test handling very large numbers"""
        very_large = 1e10  # Large but not infinity
        result = handle_numeric_value(
            self.mock_worksheet, 0, 0, very_large, None, decimal_precision=10
        )

        # Should write the large number
        args = self.mock_worksheet.write_number.call_args[0]
        assert args[2] == very_large

    def test_handle_positive_infinity(self):
        """Test handling positive infinity"""
        # Infinity will cause OverflowError in math.ceil, so it should be handled
        # The function should still try to process it, but the math.ceil will fail
        with pytest.raises(OverflowError):
            handle_numeric_value(self.mock_worksheet, 0, 0, float("inf"), None)

    def test_handle_negative_infinity(self):
        """Test handling negative infinity"""
        # Negative infinity will cause OverflowError in math.ceil
        with pytest.raises(OverflowError):
            handle_numeric_value(self.mock_worksheet, 0, 0, float("-inf"), None)


class TestSetColumnWidths:
    def setup_method(self):
        """Setup mock worksheet for each test"""
        self.mock_worksheet = Mock(spec=Worksheet)

    def test_set_none_width(self):
        """Test with None width (should do nothing)"""
        set_column_widths(self.mock_worksheet, 0, 5, None)

        self.mock_worksheet.set_column.assert_not_called()

    def test_set_single_width(self):
        """Test with single width value"""
        set_column_widths(self.mock_worksheet, 2, 3, 15)

        self.mock_worksheet.set_column.assert_called_once_with(2, 4, 15)

    def test_set_list_widths(self):
        """Test with list of widths"""
        set_column_widths(self.mock_worksheet, 0, 3, [10, 15, 20])

        expected_calls = [((0, 0, 10),), ((1, 1, 15),), ((2, 2, 20),)]
        actual_calls = self.mock_worksheet.set_column.call_args_list
        assert len(actual_calls) == 3
        for actual, expected in zip(actual_calls, expected_calls):
            assert actual[0] == expected[0]

    def test_set_list_widths_wrong_length(self):
        """Test with wrong number of widths"""
        with pytest.raises(ValueError, match="Expected 3 widths, got 2"):
            set_column_widths(self.mock_worksheet, 0, 3, [10, 15])

    def test_set_single_width_zero_columns(self):
        """Test with zero columns"""
        set_column_widths(self.mock_worksheet, 5, 0, 10)
        self.mock_worksheet.set_column.assert_called_once_with(5, 4, 10)

    def test_set_list_widths_empty_list(self):
        """Test with empty list"""
        with pytest.raises(ValueError, match="Expected 2 widths, got 0"):
            set_column_widths(self.mock_worksheet, 0, 2, [])


class TestWriteIndex:
    def setup_method(self):
        """Setup mock worksheet for each test"""
        self.mock_worksheet = Mock(spec=Worksheet)
        self.bold_format = Mock()

    def test_write_simple_index(self):
        """Test writing simple index"""
        index = pd.Index(["A", "B", "C"], name="letters")

        write_index(self.mock_worksheet, index, 2, self.bold_format)

        # Should write index name
        self.mock_worksheet.write.assert_any_call(1, 0, "letters", self.bold_format)

        # Should write index values
        expected_value_calls = [((2, 0, "A"),), ((3, 0, "B"),), ((4, 0, "C"),)]
        value_calls = [
            call
            for call in self.mock_worksheet.write.call_args_list
            if len(call[0]) == 3 and call[0][0] >= 2
        ]

        assert len(value_calls) == 3
        for actual, expected in zip(value_calls, expected_value_calls):
            assert actual[0] == expected[0]

    def test_write_multiindex(self):
        """Test writing MultiIndex"""
        index = pd.MultiIndex.from_tuples(
            [("A", 1), ("A", 2), ("B", 1)], names=["letter", "number"]
        )

        write_index(self.mock_worksheet, index, 2, self.bold_format)

        # Should write both index names
        name_calls = [
            call for call in self.mock_worksheet.write.call_args_list if call[0][0] == 1
        ]  # row_offset - 1
        assert len(name_calls) == 2

    def test_write_index_no_names(self):
        """Test writing index without names"""
        index = pd.Index(["A", "B", "C"])

        write_index(self.mock_worksheet, index, 2, self.bold_format)

        # Should not write any names (only values)
        name_calls = [
            call for call in self.mock_worksheet.write.call_args_list if call[0][0] == 1
        ]  # row_offset - 1
        assert len(name_calls) == 0

    def test_write_multiindex_partial_names(self):
        """Test writing MultiIndex with some None names"""
        index = pd.MultiIndex.from_tuples([("A", 1), ("B", 2)], names=["letter", None])

        write_index(self.mock_worksheet, index, 1, self.bold_format)

        # Should write only non-None names
        name_calls = [
            call for call in self.mock_worksheet.write.call_args_list if call[0][0] == 0
        ]
        assert len(name_calls) == 1
        assert name_calls[0][0][2] == "letter"

    def test_write_multiindex_all_none_names(self):
        """Test writing MultiIndex with all None names"""
        index = pd.MultiIndex.from_tuples([("A", 1), ("B", 2)], names=[None, None])

        write_index(self.mock_worksheet, index, 1, None)

        # Should not write any names
        name_calls = [
            call for call in self.mock_worksheet.write.call_args_list if call[0][0] == 0
        ]
        assert len(name_calls) == 0

    def test_write_empty_index(self):
        """Test writing empty index"""
        index = pd.Index([], name="empty")

        write_index(self.mock_worksheet, index, 1, self.bold_format)

        # Should write name but no values
        name_calls = [
            call for call in self.mock_worksheet.write.call_args_list if call[0][0] == 0
        ]
        assert len(name_calls) == 1

        # Should have no value calls since index is empty
        value_calls = [
            call for call in self.mock_worksheet.write.call_args_list if call[0][0] >= 1
        ]
        assert len(value_calls) == 0  # No values, only the name


class TestCreateScenarioFormats:
    def test_create_scenario_formats(self):
        """Test scenario format creation"""
        mock_workbook = Mock(spec=Workbook)
        mock_format = Mock()
        mock_workbook.add_format.return_value = mock_format

        formats = create_scenario_formats(mock_workbook)

        # Check all expected formats are created
        expected_formats = [
            "white_header",
            "grey_header",
            "white_data",
            "grey_data",
            "bold",
        ]
        for fmt in expected_formats:
            assert fmt in formats
            assert formats[fmt] == mock_format

        assert formats["default"] is None
        assert mock_workbook.add_format.call_count == 5


class TestGetScenarioBlocks:
    def test_get_scenario_blocks_simple(self):
        """Test scenario block identification"""
        columns = pd.MultiIndex.from_tuples(
            [
                ("Scenario1", "A"),
                ("Scenario1", "B"),
                ("Scenario2", "C"),
                ("Scenario2", "D"),
                ("Scenario3", "E"),
            ]
        )

        blocks = get_scenario_blocks(columns)

        expected = [("Scenario1", 0, 1), ("Scenario2", 2, 3), ("Scenario3", 4, 4)]
        assert blocks == expected

    def test_get_scenario_blocks_single_index(self):
        """Test with single-level index"""
        columns = pd.Index(["A", "B", "C"])
        blocks = get_scenario_blocks(columns)
        assert blocks == []

    def test_get_scenario_blocks_empty(self):
        """Test with empty MultiIndex"""
        columns = pd.MultiIndex.from_tuples([], names=["scenario", "variable"])
        blocks = get_scenario_blocks(columns)
        assert blocks == []

    def test_get_scenario_blocks_single_scenario(self):
        """Test with single scenario"""
        columns = pd.MultiIndex.from_tuples(
            [("OnlyScenario", "A"), ("OnlyScenario", "B"), ("OnlyScenario", "C")]
        )

        blocks = get_scenario_blocks(columns)
        expected = [("OnlyScenario", 0, 2)]
        assert blocks == expected


class TestAddSeries:
    """Test add_series function"""

    def setup_method(self):
        """Setup test data"""
        self.temp_dir = tempfile.mkdtemp()

    def teardown_method(self):
        """Clean up temp files"""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_add_simple_series(self):
        """Test adding simple Series"""
        series = pd.Series(
            [1, 2, 3, np.nan], index=["a", "b", "c", "d"], name="test_series"
        )

        file_path = os.path.join(self.temp_dir, "test_series.xlsx")
        workbook = Workbook(file_path, {"nan_inf_to_errors": True})

        worksheet = add_series("SeriesSheet", series, workbook)

        assert worksheet is not None
        assert worksheet.name == "SeriesSheet"

        workbook.close()

    def test_add_series_no_name(self):
        """Test adding Series without name"""
        series = pd.Series([1, 2, 3])

        file_path = os.path.join(self.temp_dir, "test_series_no_name.xlsx")
        workbook = Workbook(file_path, {"nan_inf_to_errors": True})

        worksheet = add_series("NoName", series, workbook)

        assert worksheet is not None
        workbook.close()

    def test_add_series_tuple_name(self):
        """Test adding Series with tuple name"""
        series = pd.Series([1, 2, 3], name=("group", "item"))

        file_path = os.path.join(self.temp_dir, "test_series_tuple.xlsx")
        workbook = Workbook(file_path, {"nan_inf_to_errors": True})

        worksheet = add_series("TupleName", series, workbook)

        assert worksheet is not None
        workbook.close()

    def test_add_series_multiindex(self):
        """Test adding Series with MultiIndex"""
        index = pd.MultiIndex.from_tuples(
            [("A", 1), ("A", 2), ("B", 1)], names=["letter", "number"]
        )

        series = pd.Series([10, 20, 30], index=index, name="values")

        file_path = os.path.join(self.temp_dir, "test_series_multi.xlsx")
        workbook = Workbook(file_path, {"nan_inf_to_errors": True})

        worksheet = add_series("MultiSeries", series, workbook)

        assert worksheet is not None
        workbook.close()

    def test_add_series_no_index(self):
        """Test adding Series without writing index"""
        series = pd.Series([1, 2, 3], name="values")

        file_path = os.path.join(self.temp_dir, "test_series_no_index.xlsx")
        workbook = Workbook(file_path, {"nan_inf_to_errors": True})

        worksheet = add_series("NoIndex", series, workbook, index=False)

        assert worksheet is not None
        workbook.close()

    def test_add_series_custom_widths(self):
        """Test adding Series with custom column widths"""
        series = pd.Series([1, 2, 3], index=["A", "B", "C"], name="values")

        file_path = os.path.join(self.temp_dir, "test_series_widths.xlsx")
        workbook = Workbook(file_path, {"nan_inf_to_errors": True})

        worksheet = add_series(
            "CustomWidths", series, workbook, column_width=20, index_width=10
        )

        assert worksheet is not None
        workbook.close()

    def test_add_series_no_freeze_panes(self):
        """Test adding Series without freezing panes"""
        series = pd.Series([1, 2, 3], name="values")

        file_path = os.path.join(self.temp_dir, "test_series_no_freeze.xlsx")
        workbook = Workbook(file_path, {"nan_inf_to_errors": True})

        worksheet = add_series("NoFreeze", series, workbook, freeze_panes=False)

        assert worksheet is not None
        workbook.close()

    def test_add_series_no_bold_headers(self):
        """Test adding Series without bold headers"""
        series = pd.Series([1, 2, 3], name="values")

        file_path = os.path.join(self.temp_dir, "test_series_no_bold.xlsx")
        workbook = Workbook(file_path, {"nan_inf_to_errors": True})

        worksheet = add_series("NoBold", series, workbook, bold_headers=False)

        assert worksheet is not None
        workbook.close()

    def test_add_series_list_name(self):
        """Test adding Series with tuple name (lists aren't hashable for Series names)"""
        # Series names must be hashable, so use tuple instead of list
        series = pd.Series([1, 2, 3], name=("part1", "part2", "part3"))

        file_path = os.path.join(self.temp_dir, "test_series_tuple_name.xlsx")
        workbook = Workbook(file_path, {"nan_inf_to_errors": True})

        worksheet = add_series("TupleName", series, workbook)

        assert worksheet is not None
        workbook.close()

    def test_add_series_multiindex_with_index_width_list(self):
        """Test adding Series with MultiIndex and list of index widths"""
        index = pd.MultiIndex.from_tuples(
            [("A", 1), ("B", 2)], names=["letter", "number"]
        )
        series = pd.Series([10, 20], index=index, name="values")

        file_path = os.path.join(self.temp_dir, "test_series_multi_widths.xlsx")
        workbook = Workbook(file_path, {"nan_inf_to_errors": True})

        worksheet = add_series("MultiWidths", series, workbook, index_width=[15, 10])

        assert worksheet is not None
        workbook.close()


class TestAddFrame:
    """Test add_frame function with comprehensive coverage"""

    def setup_method(self):
        """Setup test data"""
        self.temp_dir = tempfile.mkdtemp()

    def teardown_method(self):
        """Clean up temp files"""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_add_frame_multiindex_scenario_styling(self):
        """Test DataFrame with MultiIndex columns and scenario styling"""
        columns = pd.MultiIndex.from_tuples(
            [
                ("Scenario1", "A"),
                ("Scenario1", "B"),
                ("Scenario2", "C"),
                ("Scenario2", "D"),
            ],
            names=["scenario", "variable"],
        )

        df = pd.DataFrame(
            [[1, 2, 3, 4], [5, 6, 7, 8]], columns=columns, index=["row1", "row2"]
        )

        file_path = os.path.join(self.temp_dir, "test_multiindex_scenario.xlsx")
        workbook = Workbook(file_path, {"nan_inf_to_errors": True})

        worksheet = add_frame("MultiScenario", df, workbook, scenario_styling=True)

        assert worksheet is not None
        workbook.close()

    def test_add_frame_multiindex_no_scenario_styling(self):
        """Test DataFrame with MultiIndex columns but no scenario styling"""
        columns = pd.MultiIndex.from_tuples(
            [("Level1", "A"), ("Level1", "B"), ("Level2", "C")],
            names=["level1", "level2"],
        )

        df = pd.DataFrame([[1, 2, 3]], columns=columns)

        file_path = os.path.join(self.temp_dir, "test_multiindex_no_scenario.xlsx")
        workbook = Workbook(file_path, {"nan_inf_to_errors": True})

        worksheet = add_frame("MultiNoScenario", df, workbook, scenario_styling=False)

        assert worksheet is not None
        workbook.close()

    def test_add_frame_single_index_scenario_styling(self):
        """Test DataFrame with single-level columns and scenario styling"""
        df = pd.DataFrame(
            {"Col1": [1, 2], "Col2": [3, 4], "Col3": [5, 6], "Col4": [7, 8]}
        )

        file_path = os.path.join(self.temp_dir, "test_single_scenario.xlsx")
        workbook = Workbook(file_path, {"nan_inf_to_errors": True})

        worksheet = add_frame("SingleScenario", df, workbook, scenario_styling=True)

        assert worksheet is not None
        workbook.close()

    def test_add_frame_single_index_no_scenario_styling(self):
        """Test DataFrame with single-level columns and no scenario styling"""
        df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})

        file_path = os.path.join(self.temp_dir, "test_single_no_scenario.xlsx")
        workbook = Workbook(file_path, {"nan_inf_to_errors": True})

        worksheet = add_frame("SingleNoScenario", df, workbook, scenario_styling=False)

        assert worksheet is not None
        workbook.close()

    def test_add_frame_no_index(self):
        """Test DataFrame without writing index"""
        df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})

        file_path = os.path.join(self.temp_dir, "test_no_index.xlsx")
        workbook = Workbook(file_path, {"nan_inf_to_errors": True})

        worksheet = add_frame("NoIndex", df, workbook, index=False)

        assert worksheet is not None
        workbook.close()

    def test_add_frame_multiindex_data_index(self):
        """Test DataFrame with MultiIndex for rows"""
        index = pd.MultiIndex.from_tuples([("A", 1), ("A", 2), ("B", 1)])
        df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]}, index=index)

        file_path = os.path.join(self.temp_dir, "test_multiindex_rows.xlsx")
        workbook = Workbook(file_path, {"nan_inf_to_errors": True})

        worksheet = add_frame("MultiRows", df, workbook)

        assert worksheet is not None
        workbook.close()

    def test_add_frame_index_widths_list(self):
        """Test DataFrame with list of index widths"""
        index = pd.MultiIndex.from_tuples([("A", 1), ("B", 2)])
        df = pd.DataFrame({"col": [1, 2]}, index=index)

        file_path = os.path.join(self.temp_dir, "test_index_widths_list.xlsx")
        workbook = Workbook(file_path, {"nan_inf_to_errors": True})

        worksheet = add_frame("IndexWidthsList", df, workbook, index_width=[15, 10])

        assert worksheet is not None
        workbook.close()

    def test_add_frame_no_freeze_panes(self):
        """Test DataFrame without freezing panes"""
        df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})

        file_path = os.path.join(self.temp_dir, "test_no_freeze_panes.xlsx")
        workbook = Workbook(file_path, {"nan_inf_to_errors": True})

        worksheet = add_frame("NoFreeze", df, workbook, freeze_panes=False)

        assert worksheet is not None
        workbook.close()

    def test_add_frame_no_bold_headers(self):
        """Test DataFrame without bold headers"""
        df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})

        file_path = os.path.join(self.temp_dir, "test_no_bold.xlsx")
        workbook = Workbook(file_path, {"nan_inf_to_errors": True})

        worksheet = add_frame("NoBold", df, workbook, bold_headers=False)

        assert worksheet is not None
        workbook.close()

    def test_add_frame_custom_precision(self):
        """Test DataFrame with custom decimal precision"""
        df = pd.DataFrame({"A": [1.123456789], "B": [2.987654321]})

        file_path = os.path.join(self.temp_dir, "test_precision.xlsx")
        workbook = Workbook(file_path, {"nan_inf_to_errors": True})

        worksheet = add_frame("Precision", df, workbook, decimal_precision=3)

        assert worksheet is not None
        workbook.close()

    def test_add_frame_nan_as_text(self):
        """Test DataFrame with NaN values as text"""
        df = pd.DataFrame({"A": [1, np.nan], "B": [np.nan, 2]})

        file_path = os.path.join(self.temp_dir, "test_nan_text.xlsx")
        workbook = Workbook(file_path, {"nan_inf_to_errors": True})

        worksheet = add_frame("NaNText", df, workbook, nan_as_formula=False)

        assert worksheet is not None
        workbook.close()

    def test_add_frame_multiindex_columns_no_names(self):
        """Test DataFrame with MultiIndex columns having no names"""
        columns = pd.MultiIndex.from_tuples([("A", 1), ("B", 2)])
        df = pd.DataFrame([[1, 2]], columns=columns)

        file_path = os.path.join(self.temp_dir, "test_multiindex_no_col_names.xlsx")
        workbook = Workbook(file_path, {"nan_inf_to_errors": True})

        worksheet = add_frame("MultiNoColNames", df, workbook)

        assert worksheet is not None
        workbook.close()

    def test_add_frame_multiindex_columns_partial_names(self):
        """Test DataFrame with MultiIndex columns having partial names"""
        columns = pd.MultiIndex.from_tuples(
            [("A", 1), ("B", 2)], names=["level1", None]
        )
        df = pd.DataFrame([[1, 2]], columns=columns)

        file_path = os.path.join(
            self.temp_dir, "test_multiindex_partial_col_names.xlsx"
        )
        workbook = Workbook(file_path, {"nan_inf_to_errors": True})

        worksheet = add_frame("MultiPartialColNames", df, workbook)

        assert worksheet is not None
        workbook.close()

    def test_add_frame_single_scenario_block(self):
        """Test DataFrame with single scenario in MultiIndex"""
        columns = pd.MultiIndex.from_tuples(
            [("OnlyScenario", "A"), ("OnlyScenario", "B")]
        )
        df = pd.DataFrame([[1, 2]], columns=columns)

        file_path = os.path.join(self.temp_dir, "test_single_scenario_block.xlsx")
        workbook = Workbook(file_path, {"nan_inf_to_errors": True})

        worksheet = add_frame(
            "SingleScenarioBlock", df, workbook, scenario_styling=True
        )

        assert worksheet is not None
        workbook.close()

    def test_add_frame_empty_dataframe(self):
        """Test with empty DataFrame"""
        df = pd.DataFrame()

        file_path = os.path.join(self.temp_dir, "test_empty.xlsx")
        workbook = Workbook(file_path, {"nan_inf_to_errors": True})

        worksheet = add_frame("Empty", df, workbook)

        assert worksheet is not None
        workbook.close()


class TestIntegration:

    def setup_method(self):
        """Setup test data"""
        self.temp_dir = tempfile.mkdtemp()

    def teardown_method(self):
        """Clean up temp files"""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_full_workbook_creation(self):
        """Test creating a full workbook with multiple sheets"""
        df1 = pd.DataFrame(
            {
                "Insulation": [1000, 1200, 1500],
                "Solar": [600, 700, 900],
                "EVs": [400, 500, 600],
            },
            index=["Q1", "Q2", "Q3"],
        )
        df2 = pd.DataFrame(
            {
                "heat": [23.5, np.nan, 26.1, 22.8],
                "wind": [45.2, 48.7, 52.1, 49.3],
            },
            index=pd.date_range("2024-01-01", periods=4, freq="D"),
        )

        series = pd.Series(
            [0.1, 0.15, 0.12],
            index=["A", "B", "C"],
            name="II3050",
        )

        file_path = os.path.join(self.temp_dir, "full_test.xlsx")
        workbook = Workbook(file_path, {"nan_inf_to_errors": True})

        ws1 = add_frame("CURVES", df1, workbook, column_width=12)
        ws2 = add_frame(
            "QUERIES", df2, workbook, nan_as_formula=False, decimal_precision=1
        )
        ws3 = add_series("INPUTS", series, workbook, column_width=15)
        workbook.close()

        # Verify file was created
        assert os.path.exists(file_path)
        assert os.path.getsize(file_path) > 0

    def test_edge_cases(self):
        """Test various edge cases"""
        empty_df = pd.DataFrame()
        nan_df = pd.DataFrame({"A": [np.nan, np.nan], "B": [np.nan, np.nan]})
        single_df = pd.DataFrame({"Value": [42]})
        file_path = os.path.join(self.temp_dir, "edge_cases.xlsx")
        workbook = Workbook(file_path, {"nan_inf_to_errors": True})

        # These should not raise exceptions
        add_frame("Empty", empty_df, workbook)
        add_frame("AllNaN", nan_df, workbook)
        add_frame("Single", single_df, workbook)
        workbook.close()

        assert os.path.exists(file_path)

    def test_complex_multiindex_scenario(self):
        """Test complex scenario with multiple MultiIndex features"""
        # Create complex MultiIndex for both rows and columns
        row_index = pd.MultiIndex.from_tuples(
            [
                ("Region1", "City1"),
                ("Region1", "City2"),
                ("Region2", "City3"),
                ("Region2", "City4"),
            ],
            names=["Region", "City"],
        )

        col_index = pd.MultiIndex.from_tuples(
            [
                ("Scenario1", "Metric1"),
                ("Scenario1", "Metric2"),
                ("Scenario2", "Metric1"),
                ("Scenario2", "Metric2"),
                ("Scenario3", "Metric1"),
            ],
            names=["Scenario", "Metric"],
        )

        df = pd.DataFrame(np.random.rand(4, 5), index=row_index, columns=col_index)

        # Add some NaN values
        df.iloc[1, 2] = np.nan
        df.iloc[3, 4] = np.nan

        file_path = os.path.join(self.temp_dir, "complex_multiindex.xlsx")
        workbook = Workbook(file_path, {"nan_inf_to_errors": True})

        worksheet = add_frame(
            "ComplexMulti",
            df,
            workbook,
            scenario_styling=True,
            column_width=[12, 15, 10, 8, 20],
            index_width=[15, 12],
            decimal_precision=4,
        )

        assert worksheet is not None
        workbook.close()

    def test_all_formatting_options(self):
        """Test all formatting and styling options together"""
        df = pd.DataFrame(
            {
                "A": [1.123456789, np.nan, 3.987654321],
                "B": [np.nan, 2.555555555, 4.111111111],
                "C": [5.999999999, 6.000000001, np.nan],
            },
            index=["row1", "row2", "row3"],
        )

        file_path = os.path.join(self.temp_dir, "all_formatting.xlsx")
        workbook = Workbook(file_path, {"nan_inf_to_errors": True})

        # Test with all options enabled
        worksheet = add_frame(
            "AllFormatting",
            df,
            workbook,
            index=True,
            column_width=15,
            index_width=12,
            freeze_panes=True,
            bold_headers=True,
            nan_as_formula=True,
            decimal_precision=6,
            scenario_styling=True,
        )

        assert worksheet is not None
        workbook.close()

    def test_mixed_data_types(self):
        """Test DataFrame with mixed data types"""
        df = pd.DataFrame(
            {
                "integers": [1, 2, 3],
                "floats": [1.1, 2.2, np.nan],
                "strings": ["a", "b", "c"],
                "booleans": [True, False, True],
                "dates": pd.date_range("2024-01-01", periods=3),
            }
        )

        file_path = os.path.join(self.temp_dir, "mixed_types.xlsx")
        workbook = Workbook(file_path, {"nan_inf_to_errors": True})

        worksheet = add_frame("MixedTypes", df, workbook)

        assert worksheet is not None
        workbook.close()

    def test_very_large_precision_values(self):
        """Test with very large numbers and high precision"""
        df = pd.DataFrame(
            {
                "large_numbers": [1234567890.123456789, 9876543210.987654321],
                "small_numbers": [0.000000001, 0.000000002],
            }
        )

        file_path = os.path.join(self.temp_dir, "large_precision.xlsx")
        workbook = Workbook(file_path, {"nan_inf_to_errors": True})

        worksheet = add_frame("LargePrecision", df, workbook, decimal_precision=15)

        assert worksheet is not None
        workbook.close()


class TestErrorConditions:
    """Test error conditions and edge cases"""

    def setup_method(self):
        """Setup test data"""
        self.temp_dir = tempfile.mkdtemp()

    def teardown_method(self):
        """Clean up temp files"""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_column_width_list_mismatch(self):
        """Test error when column width list doesn't match column count"""
        df = pd.DataFrame({"A": [1], "B": [2], "C": [3]})

        file_path = os.path.join(self.temp_dir, "width_mismatch.xlsx")
        workbook = Workbook(file_path, {"nan_inf_to_errors": True})

        with pytest.raises(ValueError, match="Expected 3 widths, got 2"):
            add_frame("WidthMismatch", df, workbook, column_width=[10, 15])

        workbook.close()

    def test_index_width_list_mismatch(self):
        """Test error when index width list doesn't match index levels"""
        index = pd.MultiIndex.from_tuples([("A", 1), ("B", 2)])
        df = pd.DataFrame({"col": [1, 2]}, index=index)

        file_path = os.path.join(self.temp_dir, "index_width_mismatch.xlsx")
        workbook = Workbook(file_path, {"nan_inf_to_errors": True})

        with pytest.raises(ValueError, match="Expected 2 widths, got 1"):
            add_frame("IndexWidthMismatch", df, workbook, index_width=[10])

        workbook.close()

    def test_series_index_width_mismatch(self):
        """Test error when series index width list doesn't match index levels"""
        index = pd.MultiIndex.from_tuples([("A", 1), ("B", 2)])
        series = pd.Series([1, 2], index=index)

        file_path = os.path.join(self.temp_dir, "series_width_mismatch.xlsx")
        workbook = Workbook(file_path, {"nan_inf_to_errors": True})

        with pytest.raises(ValueError, match="Expected 2 widths, got 3"):
            add_series(
                "SeriesWidthMismatch", series, workbook, index_width=[10, 15, 20]
            )

        workbook.close()


class TestScenarioStylingEdgeCases:
    """Test edge cases in scenario styling"""

    def setup_method(self):
        """Setup test data"""
        self.temp_dir = tempfile.mkdtemp()

    def teardown_method(self):
        """Clean up temp files"""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_odd_number_of_scenario_blocks(self):
        """Test scenario styling with odd number of blocks"""
        columns = pd.MultiIndex.from_tuples(
            [("Scenario1", "A"), ("Scenario2", "B"), ("Scenario3", "C")]
        )
        df = pd.DataFrame([[1, 2, 3]], columns=columns)

        file_path = os.path.join(self.temp_dir, "odd_scenarios.xlsx")
        workbook = Workbook(file_path, {"nan_inf_to_errors": True})

        worksheet = add_frame("OddScenarios", df, workbook, scenario_styling=True)

        assert worksheet is not None
        workbook.close()

    def test_single_column_per_scenario(self):
        """Test scenario styling with single column per scenario"""
        columns = pd.MultiIndex.from_tuples(
            [
                ("Scenario1", "A"),
                ("Scenario2", "B"),
                ("Scenario3", "C"),
                ("Scenario4", "D"),
            ]
        )
        df = pd.DataFrame([[1, 2, 3, 4]], columns=columns)

        file_path = os.path.join(self.temp_dir, "single_col_scenarios.xlsx")
        workbook = Workbook(file_path, {"nan_inf_to_errors": True})

        worksheet = add_frame("SingleColScenarios", df, workbook, scenario_styling=True)

        assert worksheet is not None
        workbook.close()

    def test_uneven_scenario_blocks(self):
        """Test scenario styling with uneven block sizes"""
        columns = pd.MultiIndex.from_tuples(
            [
                ("Scenario1", "A"),
                ("Scenario1", "B"),
                ("Scenario1", "C"),  # 3 columns
                ("Scenario2", "D"),  # 1 column
                ("Scenario3", "E"),
                ("Scenario3", "F"),  # 2 columns
            ]
        )
        df = pd.DataFrame([[1, 2, 3, 4, 5, 6]], columns=columns)

        file_path = os.path.join(self.temp_dir, "uneven_scenarios.xlsx")
        workbook = Workbook(file_path, {"nan_inf_to_errors": True})

        worksheet = add_frame("UnevenScenarios", df, workbook, scenario_styling=True)

        assert worksheet is not None
        workbook.close()


class TestWorksheetNameHandling:
    """Test worksheet name handling edge cases"""

    def setup_method(self):
        """Setup test data"""
        self.temp_dir = tempfile.mkdtemp()

    def teardown_method(self):
        """Clean up temp files"""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_numeric_worksheet_name(self):
        """Test with numeric worksheet name"""
        df = pd.DataFrame({"A": [1, 2]})

        file_path = os.path.join(self.temp_dir, "numeric_name.xlsx")
        workbook = Workbook(file_path, {"nan_inf_to_errors": True})

        worksheet = add_frame(123, df, workbook)

        assert worksheet is not None
        assert worksheet.name == "123"
        workbook.close()

    def test_float_worksheet_name(self):
        """Test with float worksheet name"""
        df = pd.DataFrame({"A": [1, 2]})

        file_path = os.path.join(self.temp_dir, "float_name.xlsx")
        workbook = Workbook(file_path, {"nan_inf_to_errors": True})

        worksheet = add_frame(45.67, df, workbook)

        assert worksheet is not None
        assert worksheet.name == "45.67"
        workbook.close()

    def test_none_worksheet_name(self):
        """Test with None worksheet name"""
        df = pd.DataFrame({"A": [1, 2]})

        file_path = os.path.join(self.temp_dir, "none_name.xlsx")
        workbook = Workbook(file_path, {"nan_inf_to_errors": True})

        worksheet = add_frame(None, df, workbook)

        assert worksheet is not None
        assert worksheet.name == "None"
        workbook.close()

    def test_series_numeric_worksheet_name(self):
        """Test Series with numeric worksheet name"""
        series = pd.Series([1, 2, 3], name="values")

        file_path = os.path.join(self.temp_dir, "series_numeric_name.xlsx")
        workbook = Workbook(file_path, {"nan_inf_to_errors": True})

        worksheet = add_series(999, series, workbook)

        assert worksheet is not None
        assert worksheet.name == "999"
        workbook.close()


class TestAdditionalCoverageEdgeCases:
    """Additional tests to ensure 100% coverage"""

    def setup_method(self):
        """Setup test data"""
        self.temp_dir = tempfile.mkdtemp()

    def teardown_method(self):
        """Clean up temp files"""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_add_frame_multiindex_no_scenario_data_writing(self):
        """Test MultiIndex DataFrame data writing without scenario styling"""
        columns = pd.MultiIndex.from_tuples(
            [("Level1", "A"), ("Level1", "B"), ("Level2", "C")],
            names=["level1", "level2"],
        )

        df = pd.DataFrame(
            [[1.1, 2.2, 3.3], [4.4, 5.5, 6.6]], columns=columns, index=["row1", "row2"]
        )

        file_path = os.path.join(self.temp_dir, "test_multiindex_data_writing.xlsx")
        workbook = Workbook(file_path, {"nan_inf_to_errors": True})

        worksheet = add_frame("MultiData", df, workbook, scenario_styling=False)

        assert worksheet is not None
        workbook.close()

    def test_scenario_blocks_next_function_edge_case(self):
        """Test the next() function edge case in scenario block detection"""
        columns = pd.MultiIndex.from_tuples(
            [
                ("Scenario1", "A"),
                ("Scenario1", "B"),
                ("Scenario2", "C"),
                ("Scenario2", "D"),
            ]
        )
        df = pd.DataFrame([[1, 2, 3, 4]], columns=columns)

        file_path = os.path.join(self.temp_dir, "test_scenario_next.xlsx")
        workbook = Workbook(file_path, {"nan_inf_to_errors": True})

        # This should exercise the scenario_idx calculation in both header and data writing
        worksheet = add_frame("ScenarioNext", df, workbook, scenario_styling=True)

        assert worksheet is not None
        workbook.close()

    def test_column_width_fallback_to_default(self):
        """Test when index_width falls back to column_width"""
        df = pd.DataFrame({"A": [1, 2]}, index=["x", "y"])

        file_path = os.path.join(self.temp_dir, "test_width_fallback.xlsx")
        workbook = Workbook(file_path, {"nan_inf_to_errors": True})

        # index_width=None should fall back to column_width
        worksheet = add_frame(
            "WidthFallback", df, workbook, column_width=20, index_width=None
        )

        assert worksheet is not None
        workbook.close()

    def test_series_column_width_setting(self):
        """Test series column width setting when column_width is None"""
        series = pd.Series([1, 2, 3], name="test")

        file_path = os.path.join(self.temp_dir, "test_series_no_width.xlsx")
        workbook = Workbook(file_path, {"nan_inf_to_errors": True})

        worksheet = add_series("SeriesNoWidth", series, workbook, column_width=None)

        assert worksheet is not None
        workbook.close()

    def test_series_index_width_fallback(self):
        """Test series index width fallback to column width"""
        series = pd.Series([1, 2, 3], index=["a", "b", "c"], name="test")

        file_path = os.path.join(self.temp_dir, "test_series_index_fallback.xlsx")
        workbook = Workbook(file_path, {"nan_inf_to_errors": True})

        # index_width=None should fall back to column_width
        worksheet = add_series(
            "SeriesIndexFallback", series, workbook, column_width=15, index_width=None
        )

        assert worksheet is not None
        workbook.close()

    def test_get_scenario_blocks_edge_coverage(self):
        """Test get_scenario_blocks function edge cases for complete coverage"""
        # Test completely empty MultiIndex
        empty_columns = pd.MultiIndex.from_tuples([], names=["scenario", "variable"])
        blocks = get_scenario_blocks(empty_columns)
        assert blocks == []

        # Test with repeating scenario changes
        columns = pd.MultiIndex.from_tuples(
            [("A", "1"), ("B", "1"), ("A", "2"), ("B", "2"), ("A", "3")]
        )
        blocks = get_scenario_blocks(columns)
        expected = [("A", 0, 0), ("B", 1, 1), ("A", 2, 2), ("B", 3, 3), ("A", 4, 4)]
        assert blocks == expected


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
