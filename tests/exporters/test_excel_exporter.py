"""Tests for ExcelExporter and related writer classes."""

import pytest
import pandas as pd
from pathlib import Path
import tempfile
from xlsxwriter import Workbook

from pyetm.exporters.excel_exporter import (
    ExcelExporter,
    PathManager,
    MainSheetWriter,
    DataSheetWriter,
    HourlyCurvesWriter,
    AnnualExportsWriter,
)
from pyetm.models.export_data_collection import ExportDataCollection
from pyetm.models.export_config import ExportConfig


@pytest.fixture
def minimal_export_data():
    """Create minimal export data collection."""
    main_info = pd.DataFrame({
        "scenario_id": [1, 2],
        "identifier": ["test1", "test2"],
        "title": ["Test 1", "Test 2"],
    })

    config = ExportConfig(
        include_inputs=False,
        include_sortables=False,
        include_custom_curves=False,
        include_gqueries=False,
        include_users=False,
    )

    return ExportDataCollection(main_info=main_info, config=config)


@pytest.fixture
def full_export_data():
    """Create export data with all fields populated."""
    main_info = pd.DataFrame({
        "scenario_id": [1, 2],
        "identifier": ["test1", "test2"],
        "title": ["Test 1", "Test 2"],
    })

    inputs = pd.DataFrame({
        ("test1", "user"): [100, 200],
        ("test2", "user"): [150, 250],
    }, index=["input1", "input2"])

    sortables = pd.DataFrame({
        "test1": [1.0, 2.0],
        "test2": [1.5, 2.5],
    }, index=["tech1", "tech2"])

    custom_curves = {
        "curve1": {
            "test1": pd.Series([1, 2, 3], name="curve1"),
            "test2": pd.Series([4, 5, 6], name="curve1"),
        }
    }

    hourly_output_curves = {
        "electricity_production": {
            "test1": pd.DataFrame({"value": range(8760)}),
            "test2": pd.DataFrame({"value": range(8760)}),
        }
    }

    annual_exports = {
        "energy_flow": {
            "test1": pd.DataFrame({"carrier": ["electricity"], "value": [100]}),
            "test2": pd.DataFrame({"carrier": ["gas"], "value": [200]}),
        }
    }

    config = ExportConfig(
        include_inputs=True,
        include_sortables=True,
        include_custom_curves=True,
        include_gqueries=False,
        include_users=False,
        output_carriers=["electricity"],
        include_annual_exports=["energy_flow"],
    )

    return ExportDataCollection(
        main_info=main_info,
        inputs=inputs,
        sortables=sortables,
        custom_curves=custom_curves,
        hourly_output_curves=hourly_output_curves,
        annual_exports=annual_exports,
        config=config,
    )


class TestPathManager:
    """Test PathManager utility class."""

    def test_ensure_directory_exists_creates_directory(self, tmp_path):
        """Test that directory creation works."""
        test_file = tmp_path / "subdir" / "test.xlsx"
        PathManager.ensure_directory_exists(str(test_file))
        assert test_file.parent.exists()

    def test_ensure_directory_exists_handles_existing(self, tmp_path):
        """Test that existing directory doesn't cause error."""
        test_file = tmp_path / "test.xlsx"
        PathManager.ensure_directory_exists(str(test_file))
        PathManager.ensure_directory_exists(str(test_file))  # Should not raise
        assert test_file.parent.exists()

    def test_get_hourly_curves_path(self, tmp_path):
        """Test hourly curves path generation."""
        main_path = tmp_path / "export.xlsx"
        hourly_path = PathManager.get_hourly_curves_path(main_path)
        assert hourly_path == tmp_path / "export_hourly_output_curves.xlsx"

    def test_get_annual_exports_path(self, tmp_path):
        """Test annual exports path generation."""
        main_path = tmp_path / "export.xlsx"
        annual_path = PathManager.get_annual_exports_path(main_path)
        assert annual_path == tmp_path / "export_annual_exports.xlsx"


class TestMainSheetWriter:
    """Test MainSheetWriter class."""

    def test_write_with_empty_dataframe(self, tmp_path):
        """Test that empty DataFrame doesn't create sheet."""
        workbook_path = tmp_path / "test.xlsx"
        workbook = Workbook(str(workbook_path))

        empty_df = pd.DataFrame()
        MainSheetWriter.write(workbook, empty_df, [])

        workbook.close()

        # Verify file was created but is small (no data written)
        assert workbook_path.exists()

    def test_write_with_valid_data(self, tmp_path):
        """Test writing valid main info data."""
        workbook_path = tmp_path / "test.xlsx"
        workbook = Workbook(str(workbook_path))

        main_info = pd.DataFrame({
            "scenario_id": [1, 2],
            "identifier": ["test1", "test2"],
            "title": ["Test 1", "Test 2"],
        })

        # Mock scenarios
        class MockScenario:
            def __init__(self, id_val):
                self.id = id_val

            def identifier(self):
                return f"test{self.id}"

        scenarios = [MockScenario(1), MockScenario(2)]

        MainSheetWriter.write(workbook, main_info, scenarios)
        workbook.close()

        # Verify Excel file was created
        assert workbook_path.exists()

        # Verify sheet was created
        excel_file = pd.ExcelFile(str(workbook_path))
        assert "MAIN" in excel_file.sheet_names


class TestDataSheetWriter:
    """Test DataSheetWriter class."""

    def test_write_inputs_with_none(self, tmp_path):
        """Test that None inputs don't cause error."""
        workbook_path = tmp_path / "test.xlsx"
        workbook = Workbook(str(workbook_path))
        DataSheetWriter.write_inputs(workbook, None)
        workbook.close()
        assert workbook_path.exists()

    def test_write_inputs_with_data(self, tmp_path):
        """Test writing inputs data."""
        workbook_path = tmp_path / "test.xlsx"
        workbook = Workbook(str(workbook_path))

        inputs = pd.DataFrame({
            ("test1", "user"): [100, 200],
            ("test2", "user"): [150, 250],
        }, index=["input1", "input2"])

        DataSheetWriter.write_inputs(workbook, inputs)
        workbook.close()

        excel_file = pd.ExcelFile(str(workbook_path))
        assert "SLIDER_SETTINGS" in excel_file.sheet_names

    def test_combine_custom_curves_empty(self):
        """Test combining empty custom curves."""
        result = DataSheetWriter._combine_custom_curves({})
        assert result is None

    def test_combine_custom_curves_with_data(self):
        """Test combining custom curves into DataFrame."""
        curves = {
            "curve1": {
                "test1": pd.Series([1, 2, 3], name="curve1"),
                "test2": pd.Series([4, 5, 6], name="curve1"),
            }
        }

        result = DataSheetWriter._combine_custom_curves(curves)
        assert result is not None
        assert isinstance(result, pd.DataFrame)
        assert result.shape == (3, 2)
        assert isinstance(result.columns, pd.MultiIndex)


class TestHourlyCurvesWriter:
    """Test HourlyCurvesWriter class."""

    def test_write_with_empty_data(self, tmp_path, caplog):
        """Test writing empty curves data."""
        import logging
        caplog.set_level(logging.INFO)
        output_path = tmp_path / "curves.xlsx"
        HourlyCurvesWriter.write({}, output_path, None)
        assert "No hourly curves data available" in caplog.text

    def test_normalize_to_series_with_series(self):
        """Test normalizing a Series."""
        series = pd.Series([1, 2, 3], name="test")
        result = HourlyCurvesWriter._normalize_to_series(series, "scenario1", "curve1")
        assert len(result) == 1
        assert result[0][0] == ("scenario1", "curve1")
        assert isinstance(result[0][1], pd.Series)

    def test_normalize_to_series_with_single_column_dataframe(self):
        """Test normalizing single-column DataFrame."""
        df = pd.DataFrame({"value": [1, 2, 3]})
        result = HourlyCurvesWriter._normalize_to_series(df, "scenario1", "curve1")
        assert len(result) == 1
        assert result[0][0] == ("scenario1", "curve1")

    def test_normalize_to_series_with_multi_column_dataframe(self):
        """Test normalizing multi-column DataFrame."""
        df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
        result = HourlyCurvesWriter._normalize_to_series(df, "scenario1", "curve1")
        assert len(result) == 2
        assert result[0][0] == ("scenario1", "curve1:col1")
        assert result[1][0] == ("scenario1", "curve1:col2")

    def test_select_carriers_with_none(self):
        """Test carrier selection with None."""
        carrier_map = {"electricity": ["curve1"], "heat": ["curve2"]}
        result = HourlyCurvesWriter._select_carriers(carrier_map, None)
        assert set(result) == {"electricity", "heat"}

    def test_select_carriers_with_specific_carriers(self):
        """Test carrier selection with specific carriers."""
        carrier_map = {"electricity": ["curve1"], "heat": ["curve2"], "hydrogen": ["curve3"]}
        result = HourlyCurvesWriter._select_carriers(carrier_map, ["electricity", "heat"])
        assert set(result) == {"electricity", "heat"}


class TestAnnualExportsWriter:
    """Test AnnualExportsWriter class."""

    def test_write_with_empty_data(self, tmp_path, caplog):
        """Test writing empty exports data."""
        import logging
        caplog.set_level(logging.INFO)
        output_path = tmp_path / "exports.xlsx"
        AnnualExportsWriter.write({}, output_path)
        assert "No export data available" in caplog.text

    def test_write_with_valid_data(self, tmp_path):
        """Test writing valid exports data."""
        output_path = tmp_path / "exports.xlsx"
        exports_data = {
            "energy_flow": {
                "test1": pd.DataFrame({"carrier": ["electricity"], "value": [100]}),
                "test2": pd.DataFrame({"carrier": ["gas"], "value": [200]}),
            }
        }

        AnnualExportsWriter.write(exports_data, output_path)

        assert output_path.exists()
        excel_file = pd.ExcelFile(str(output_path))
        assert "ENERGY_FLOW" in excel_file.sheet_names

    def test_write_export_sheet_adds_scenario_column(self, tmp_path):
        """Test that scenario column is added to export sheets."""
        output_path = tmp_path / "exports.xlsx"
        exports_data = {
            "energy_flow": {
                "test1": pd.DataFrame({"carrier": ["electricity"], "value": [100]}),
            }
        }

        AnnualExportsWriter.write(exports_data, output_path)

        df = pd.read_excel(str(output_path), sheet_name="ENERGY_FLOW")
        assert "scenario" in df.columns
        assert df["scenario"].iloc[0] == "test1"


class TestExcelExporter:
    """Test main ExcelExporter class."""

    def test_write_minimal_export(self, tmp_path, minimal_export_data):
        """Test writing minimal export data."""
        output_path = tmp_path / "export.xlsx"

        # Mock scenarios
        class MockScenario:
            def __init__(self, id_val):
                self.id = id_val

            def identifier(self):
                return f"test{self.id}"

        scenarios = [MockScenario(1), MockScenario(2)]

        result_path = ExcelExporter.write(minimal_export_data, str(output_path), scenarios)

        assert result_path == output_path
        assert output_path.exists()

        excel_file = pd.ExcelFile(str(output_path))
        assert "MAIN" in excel_file.sheet_names

    def test_write_full_export(self, tmp_path, full_export_data, monkeypatch):
        """Test writing export with all data types."""
        # Mock carrier mappings to avoid file dependency
        mock_carrier_map = {
            "electricity": ["electricity_production", "electricity_import"],
            "heat": ["heat_production"],
        }

        def mock_load_carrier_mappings():
            return mock_carrier_map

        from pyetm.models import hourly_output_curves
        monkeypatch.setattr(
            hourly_output_curves.HourlyOutputCurves,
            "_load_carrier_mappings",
            lambda: mock_carrier_map
        )

        output_path = tmp_path / "export.xlsx"

        class MockScenario:
            def __init__(self, id_val):
                self.id = id_val

            def identifier(self):
                return f"test{self.id}"

        scenarios = [MockScenario(1), MockScenario(2)]

        result_path = ExcelExporter.write(full_export_data, str(output_path), scenarios)

        assert result_path == output_path
        assert output_path.exists()

        excel_file = pd.ExcelFile(str(output_path))
        assert "MAIN" in excel_file.sheet_names
        assert "SLIDER_SETTINGS" in excel_file.sheet_names
        assert "SORTABLES" in excel_file.sheet_names
        assert "CUSTOM_CURVES" in excel_file.sheet_names

        # Check separate workbooks
        hourly_path = PathManager.get_hourly_curves_path(output_path)
        annual_path = PathManager.get_annual_exports_path(output_path)

        assert hourly_path.exists()
        assert annual_path.exists()

    def test_write_creates_parent_directory(self, tmp_path):
        """Test that parent directory is created if needed."""
        output_path = tmp_path / "subdir" / "nested" / "export.xlsx"

        main_info = pd.DataFrame({
            "scenario_id": [1],
            "identifier": ["test1"],
        })

        config = ExportConfig(
            include_inputs=False,
            include_sortables=False,
            include_custom_curves=False,
            include_gqueries=False,
            include_users=False,
        )

        export_data = ExportDataCollection(main_info=main_info, config=config)

        class MockScenario:
            id = 1

            def identifier(self):
                return "test1"

        ExcelExporter.write(export_data, str(output_path), [MockScenario()])

        assert output_path.exists()
        assert output_path.parent.exists()

    def test_write_with_detailed_inputs(self, tmp_path):
        """Test writing with detailed inputs sheet."""
        main_info = pd.DataFrame({
            "scenario_id": [1],
            "identifier": ["test1"],
        })

        inputs = pd.DataFrame({
            ("test1", "user"): [100],
        }, index=["input1"])

        inputs_detailed = pd.DataFrame({
            ("test1", "user"): [100],
            ("test1", "default"): [50],
            ("test1", "min"): [0],
            ("test1", "max"): [200],
        }, index=["input1"])

        config = ExportConfig(
            include_inputs=True,
            include_sortables=False,
            include_custom_curves=False,
            include_gqueries=False,
            include_users=False,
        )

        export_data = ExportDataCollection(
            main_info=main_info,
            inputs=inputs,
            inputs_detailed=inputs_detailed,
            config=config,
        )

        class MockScenario:
            id = 1

            def identifier(self):
                return "test1"

        output_path = tmp_path / "export.xlsx"
        ExcelExporter.write(export_data, str(output_path), [MockScenario()])

        excel_file = pd.ExcelFile(str(output_path))
        assert "SLIDER_SETTINGS" in excel_file.sheet_names
        assert "INPUT_DETAILS" in excel_file.sheet_names


class TestIntegration:
    """Integration tests for the full export workflow."""

    def test_full_export_workflow(self, tmp_path, full_export_data, monkeypatch):
        """Test complete export workflow from collection to files."""
        # Mock carrier mappings to avoid file dependency
        mock_carrier_map = {
            "electricity": ["electricity_production", "electricity_import"],
            "heat": ["heat_production"],
        }

        from pyetm.models import hourly_output_curves
        monkeypatch.setattr(
            hourly_output_curves.HourlyOutputCurves,
            "_load_carrier_mappings",
            lambda: mock_carrier_map
        )

        output_path = tmp_path / "full_export.xlsx"

        class MockScenario:
            def __init__(self, id_val):
                self.id = id_val

            def identifier(self):
                return f"test{self.id}"

        scenarios = [MockScenario(1), MockScenario(2)]

        # Export to Excel
        result = ExcelExporter.write(full_export_data, str(output_path), scenarios)

        # Verify main file
        assert result == output_path
        assert output_path.exists()

        # Verify all sheets in main file
        main_excel = pd.ExcelFile(str(output_path))
        expected_sheets = ["MAIN", "SLIDER_SETTINGS", "SORTABLES", "CUSTOM_CURVES"]
        for sheet in expected_sheets:
            assert sheet in main_excel.sheet_names

        # Verify separate workbooks
        hourly_path = PathManager.get_hourly_curves_path(output_path)
        annual_path = PathManager.get_annual_exports_path(output_path)

        assert hourly_path.exists()
        assert annual_path.exists()

        # Verify hourly curves content
        hourly_excel = pd.ExcelFile(str(hourly_path))
        assert "ELECTRICITY" in hourly_excel.sheet_names

        # Verify annual exports content
        annual_excel = pd.ExcelFile(str(annual_path))
        assert "ENERGY_FLOW" in annual_excel.sheet_names
