import pytest
import pandas as pd
import numpy as np
import tempfile
import os
from unittest.mock import Mock
from xlsxwriter.workbook import Workbook
from xlsxwriter.worksheet import Worksheet

from pyetm.utils.excel import (
    handle_numeric_value,
    set_column_widths,
    write_index,
    add_frame,
    add_series,
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


class TestAddFrame:
    """Test add_frame function"""

    def setup_method(self):
        """Setup test data"""
        self.temp_dir = tempfile.mkdtemp()

    def teardown_method(self):
        """Clean up temp files"""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_add_simple_dataframe(self):
        """Test adding simple DataFrame"""
        df = pd.DataFrame(
            {"A": [1, 2, 3], "B": [4.5, np.nan, 6.7]}, index=["row1", "row2", "row3"]
        )

        file_path = os.path.join(self.temp_dir, "test.xlsx")
        workbook = Workbook(file_path, {"nan_inf_to_errors": True})

        worksheet = add_frame("TestSheet", df, workbook)

        assert worksheet is not None
        assert worksheet.name == "TestSheet"

        workbook.close()

    def test_add_dataframe_no_index(self):
        """Test adding DataFrame without index"""
        df = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})

        file_path = os.path.join(self.temp_dir, "test_no_index.xlsx")
        workbook = Workbook(file_path, {"nan_inf_to_errors": True})

        worksheet = add_frame("TestSheet", df, workbook, index=False)

        assert worksheet is not None
        workbook.close()

    def test_add_multiindex_dataframe(self):
        """Test adding DataFrame with MultiIndex"""
        arrays = [["A", "A", "B", "B"], [1, 2, 1, 2]]
        index = pd.MultiIndex.from_arrays(arrays, names=["letter", "number"])

        columns = pd.MultiIndex.from_tuples(
            [("X", "col1"), ("X", "col2"), ("Y", "col1")], names=["group", "item"]
        )

        df = pd.DataFrame(np.random.randn(4, 3), index=index, columns=columns)

        file_path = os.path.join(self.temp_dir, "test_multi.xlsx")
        workbook = Workbook(file_path, {"nan_inf_to_errors": True})

        worksheet = add_frame("MultiTest", df, workbook)

        assert worksheet is not None
        workbook.close()

    def test_add_frame_with_custom_options(self):
        """Test add_frame with custom options"""
        df = pd.DataFrame({"A": [1.123456789, 2.987654321], "B": [np.nan, 4.555555555]})

        file_path = os.path.join(self.temp_dir, "test_custom.xlsx")
        workbook = Workbook(file_path, {"nan_inf_to_errors": True})

        worksheet = add_frame(
            "CustomTest",
            df,
            workbook,
            column_width=[15, 20],
            index_width=12,
            freeze_panes=False,
            bold_headers=False,
            nan_as_formula=False,
            decimal_precision=3,
        )

        assert worksheet is not None
        workbook.close()


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
