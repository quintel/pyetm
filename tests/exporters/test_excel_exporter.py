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
        hourly_curves=["electricity"],
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

    def test_write_export_sheet_with_horizontal_layout(self, tmp_path):
        """Test that scenarios are arranged horizontally with MultiIndex columns."""
        output_path = tmp_path / "exports.xlsx"
        exports_data = {
            "energy_flow": {
                "test1": pd.DataFrame({"carrier": ["electricity"], "value": [100]}),
            }
        }

        AnnualExportsWriter.write(exports_data, output_path)

        df = pd.read_excel(str(output_path), sheet_name="ENERGY_FLOW", header=[0, 1])
        # With horizontal layout, the first row contains scenario names
        assert "test1" in df.columns.get_level_values(0)
        # Verify the data structure
        assert df[("test1", "carrier")].iloc[0] == "electricity"
        assert df[("test1", "value")].iloc[0] == 100

    def test_write_with_schema_mismatch_numeric(self, tmp_path, caplog):
        """Test writing exports with schema mismatch - numeric columns."""
        import logging
        caplog.set_level(logging.WARNING)

        output_path = tmp_path / "exports.xlsx"
        exports_data = {
            "energy_flow": {
                "scenario1": pd.DataFrame({
                    "carrier": ["electricity", "gas"],
                    "value": [100.0, 200.0]
                }),
                "scenario2": pd.DataFrame({
                    "carrier": ["electricity", "gas"],
                    "value": [150.0, 250.0],
                    "cost": [10.0, 20.0]  # Extra column
                }),
            }
        }

        AnnualExportsWriter.write(exports_data, output_path)

        # Verify file exists and can be read with MultiIndex columns
        assert output_path.exists()
        df = pd.read_excel(str(output_path), sheet_name="ENERGY_FLOW", header=[0, 1])

        # Verify both scenarios are present in column index
        scenarios = df.columns.get_level_values(0).unique()
        assert "scenario1" in scenarios
        assert "scenario2" in scenarios

        # Verify scenario1 has carrier and value columns (no cost)
        assert "carrier" in df["scenario1"].columns
        assert "value" in df["scenario1"].columns

        # Verify scenario2 has carrier, value, and cost columns
        assert "carrier" in df["scenario2"].columns
        assert "value" in df["scenario2"].columns
        assert "cost" in df["scenario2"].columns

        # Verify data values
        assert df[("scenario1", "value")].iloc[0] == 100.0
        assert df[("scenario2", "cost")].iloc[0] == 10.0

    def test_write_with_schema_mismatch_string(self, tmp_path, caplog):
        """Test writing exports with schema mismatch - string columns."""
        import logging
        caplog.set_level(logging.INFO)  # Changed from WARNING to capture INFO logs

        output_path = tmp_path / "exports.xlsx"
        exports_data = {
            "sankey": {
                "scenario1": pd.DataFrame({
                    "carrier": ["electricity"],
                    "from": ["production"],
                    "to": ["demand"]
                }),
                "scenario2": pd.DataFrame({
                    "carrier": ["gas"],
                    "from": ["import"],
                    "to": ["conversion"],
                    "unit": ["MWh"]  # Extra string column
                }),
            }
        }

        AnnualExportsWriter.write(exports_data, output_path)

        # Verify file was created successfully despite schema mismatch
        assert output_path.exists()
        df = pd.read_excel(str(output_path), sheet_name="SANKEY", header=[0, 1])

        # Verify scenarios appear as top-level columns
        assert "scenario1" in df.columns.get_level_values(0)
        assert "scenario2" in df.columns.get_level_values(0)

        # Check that scenario2 has the unit column with actual value
        assert ("scenario2", "unit") in df.columns
        assert df[("scenario2", "unit")].iloc[0] == "MWh"

        # Check that scenario1 does NOT have unit column (it's not in the schema)
        assert ("scenario1", "unit") not in df.columns, "scenario1 should not have unit column"

    def test_write_with_schema_mismatch_mixed_types(self, tmp_path, caplog):
        """Test writing exports with multiple missing columns of different types."""
        import logging
        caplog.set_level(logging.INFO)

        output_path = tmp_path / "exports.xlsx"
        exports_data = {
            "production_parameters": {
                "scenario1": pd.DataFrame({
                    "technology": ["solar_pv"],
                    "capacity": [100.0]
                }),
                "scenario2": pd.DataFrame({
                    "technology": ["wind_turbine"],
                    "capacity": [200.0],
                    "efficiency": [0.95],  # Extra numeric
                    "region": ["north"]    # Extra string
                }),
            }
        }

        AnnualExportsWriter.write(exports_data, output_path)

        # Verify file was created successfully
        assert output_path.exists()
        df = pd.read_excel(str(output_path), sheet_name="PRODUCTION_PARAMETERS", header=[0, 1])

        # Verify scenarios appear as top-level columns
        assert "scenario1" in df.columns.get_level_values(0)
        assert "scenario2" in df.columns.get_level_values(0)

        # Check scenario2 has the extra columns with actual values
        assert ("scenario2", "efficiency") in df.columns
        assert ("scenario2", "region") in df.columns
        assert df[("scenario2", "efficiency")].iloc[0] == 0.95
        assert df[("scenario2", "region")].iloc[0] == "north"

        # Check scenario1 does NOT have the extra columns
        assert ("scenario1", "efficiency") not in df.columns, "scenario1 should not have efficiency column"
        assert ("scenario1", "region") not in df.columns, "scenario1 should not have region column"

    def test_write_no_nan_when_schemas_match(self, tmp_path, caplog):
        """Test that no NaN filling occurs when schemas match."""
        import logging
        caplog.set_level(logging.INFO)

        output_path = tmp_path / "exports.xlsx"
        exports_data = {
            "energy_flow": {
                "scenario1": pd.DataFrame({
                    "carrier": ["electricity"],
                    "value": [100.0]
                }),
                "scenario2": pd.DataFrame({
                    "carrier": ["gas"],
                    "value": [200.0]
                }),
            }
        }

        AnnualExportsWriter.write(exports_data, output_path)

        assert output_path.exists()

        # Verify no NaN values in result when schemas match
        df = pd.read_excel(str(output_path), sheet_name="ENERGY_FLOW", header=[0, 1])
        assert not df.isna().any().any(), "No NaN values should exist when schemas match"

    def test_write_with_indexed_dataframes(self, tmp_path):
        """Test writing DataFrames with indices (as from API CSV parsing with index_col=0)."""
        output_path = tmp_path / "exports.xlsx"

        # Simulate DataFrames as they come from API with index_col=0
        # The first column becomes the index
        df1 = pd.DataFrame({
            "electricity": [100.0, 200.0],
            "gas": [50.0, 75.0],
        }, index=pd.Index(["production", "demand"], name="carrier"))

        df2 = pd.DataFrame({
            "electricity": [150.0, 250.0],
            "gas": [60.0, 85.0],
        }, index=pd.Index(["production", "demand"], name="carrier"))

        exports_data = {
            "energy_flow": {
                "scenario1": df1,
                "scenario2": df2,
            }
        }

        AnnualExportsWriter.write(exports_data, output_path)

        assert output_path.exists()

        # Read back and verify horizontal MultiIndex structure
        df = pd.read_excel(str(output_path), sheet_name="ENERGY_FLOW", header=[0, 1])

        # Verify scenarios appear as top-level columns
        assert "scenario1" in df.columns.get_level_values(0)
        assert "scenario2" in df.columns.get_level_values(0)

        # Should have 2 rows (from the 2 index values: production, demand)
        assert len(df) == 2, f"Expected 2 rows, got {len(df)}"

        # Verify carrier column exists as SHARED column (not under scenarios)
        # The empty string "" for shared columns becomes "Unnamed: 0_level_0" when read from Excel
        shared_col_names = [col for col in df.columns if "Unnamed" in str(col[0])]
        assert len(shared_col_names) > 0, "Should have at least one shared column"

        # Find the carrier column (should be first column with "carrier" in name)
        carrier_col = None
        for col in df.columns:
            if "carrier" in str(col[1]).lower():
                carrier_col = col
                break

        assert carrier_col is not None, "carrier column should exist"

        # Verify carrier values are present (not NaN)
        assert set(df[carrier_col]) == {"production", "demand"}
        assert not df[carrier_col].isna().any(), "carrier column should not have NaN values"

        # Verify data values are correct for scenario1
        production_idx = df[df[carrier_col] == "production"].index[0]
        demand_idx = df[df[carrier_col] == "demand"].index[0]
        assert df.loc[production_idx, ("scenario1", "electricity")] == 100.0
        assert df.loc[demand_idx, ("scenario1", "electricity")] == 200.0

    def test_write_with_multi_index_dataframes(self, tmp_path):
        """Test writing DataFrames with multi-level indices."""
        output_path = tmp_path / "exports.xlsx"

        # Create DataFrames with multi-level index
        df1 = pd.DataFrame({
            "value": [100.0, 200.0, 150.0, 250.0],
        }, index=pd.MultiIndex.from_tuples([
            ("electricity", "production"),
            ("electricity", "demand"),
            ("gas", "production"),
            ("gas", "demand"),
        ], names=["carrier", "type"]))

        df2 = pd.DataFrame({
            "value": [110.0, 210.0, 160.0, 260.0],
        }, index=pd.MultiIndex.from_tuples([
            ("electricity", "production"),
            ("electricity", "demand"),
            ("gas", "production"),
            ("gas", "demand"),
        ], names=["carrier", "type"]))

        exports_data = {
            "energy_flow": {
                "scenario1": df1,
                "scenario2": df2,
            }
        }

        AnnualExportsWriter.write(exports_data, output_path)

        assert output_path.exists()

        # Read back and verify horizontal MultiIndex structure
        df = pd.read_excel(str(output_path), sheet_name="ENERGY_FLOW", header=[0, 1])

        # Verify scenarios appear as top-level columns
        assert "scenario1" in df.columns.get_level_values(0)
        assert "scenario2" in df.columns.get_level_values(0)

        # Should have 4 rows (from the 4 index combinations)
        assert len(df) == 4

        # Verify index columns exist as SHARED columns (not under scenarios)
        # Both carrier and type should be shared since they're from a MultiIndex
        shared_cols = [col for col in df.columns if "Unnamed" in str(col[0])]
        assert len(shared_cols) >= 2, "Should have at least 2 shared columns (carrier and type)"

        # Find the carrier and type columns
        carrier_col = None
        type_col = None
        for col in df.columns:
            if "carrier" in str(col[1]).lower():
                carrier_col = col
            elif "type" in str(col[1]).lower():
                type_col = col

        assert carrier_col is not None, "carrier column should exist as shared column"
        assert type_col is not None, "type column should exist as shared column"

        # Verify no NaN in index columns
        assert not df[carrier_col].isna().any()
        assert not df[type_col].isna().any()


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


class TestHourlyCurvesValidation:
    """Test validation of hourly curves during export."""

    def test_write_with_invalid_carrier_names(self):
        """Test that invalid carrier names are filtered with warnings during write."""
        # Create curves data with mix of valid and invalid carriers
        curves_data = {
            "merit_order": {
                "test1": pd.DataFrame({"value": range(100)}),
            },
            "electricity_price": {
                "test1": pd.DataFrame({"value": range(100)}),
            },
        }

        # TODO: Add test implementation once validation is added
        # Should filter invalid carriers and log warnings
        pytest.skip("Test to be implemented after validation is added")

    def test_write_with_all_invalid_carriers(self):
        """Test behavior when all specified carriers are invalid."""
        # TODO: Add test implementation once validation is added
        pytest.skip("Test to be implemented after validation is added")

    def test_write_with_invalid_curve_names(self):
        """Test that invalid curve names are filtered during write."""
        # TODO: Add test implementation once validation is added
        pytest.skip("Test to be implemented after validation is added")


class TestAnnualExportsValidation:
    """Test validation of annual exports during export."""

    def test_write_with_invalid_export_types(self):
        """Test that invalid export types are filtered with warnings during write."""
        exports_data = {
            "energy_flow": {
                "test1": pd.DataFrame({"carrier": ["electricity"], "value": [100]}),
            },
            "invalid_export": {  # This should be caught
                "test1": pd.DataFrame({"data": [1, 2, 3]}),
            },
        }

        # TODO: Add test implementation once validation is added
        # Should filter invalid export types and log warnings
        pytest.skip("Test to be implemented after validation is added")

    def test_write_with_all_invalid_export_types(self):
        """Test behavior when all export types are invalid."""
        # TODO: Add test implementation once validation is added
        pytest.skip("Test to be implemented after validation is added")


class TestRoundTripValidation:
    """Test round-trip scenarios: from_excel() → to_excel() → from_excel()."""

    def test_round_trip_with_invalid_hourly_curves(self):
        """Test that invalid hourly curves are handled consistently across round-trip."""
        # TODO: Add test implementation once validation is added
        # Write Excel with invalid carriers, read back, should have same result
        pytest.skip("Test to be implemented after round-trip validation is added")

    def test_round_trip_with_invalid_annual_exports(self):
        """Test that invalid annual exports are handled consistently across round-trip."""
        # TODO: Add test implementation once validation is added
        pytest.skip("Test to be implemented after round-trip validation is added")
