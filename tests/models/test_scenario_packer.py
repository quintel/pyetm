import pytest
import pandas as pd
import tempfile
import os
from unittest.mock import Mock, patch
from pyetm.models.scenario_packer import (
    ScenarioPacker,
    ExportConfigResolver,
)
from pyetm.models.packables.custom_curves_pack import CustomCurvesPack
from pyetm.models.packables.inputs_pack import InputsPack
from pyetm.models.packables.output_curves_pack import OutputCurvesPack
from pyetm.models.packables.sortable_pack import SortablePack
from pyetm.models.packables.query_pack import QueryPack
from pyetm.models import Scenario
from pyetm.models.custom_curves import CustomCurves


class TestScenarioPackerInit:

    def test_init_creates_empty_collections(self):
        """Test that initialization creates empty collections"""
        packer = ScenarioPacker()

        assert isinstance(packer._scenarios(), set)
        assert len(packer._scenarios()) == 0


class TestScenarioPackerAdd:

    def test_add_single_scenario(self, sample_scenario):
        """Test adding a single scenario"""
        packer = ScenarioPacker()
        packer.add(sample_scenario)

        # Should be added to all collections
        for pack in packer._get_all_packs():
            assert sample_scenario in pack.scenarios

    def test_add_multiple_scenarios(self, multiple_scenarios):
        """Test adding multiple scenarios at once"""
        packer = ScenarioPacker()
        packer.add(*multiple_scenarios)

        # All scenarios should be in all collections
        for pack in packer._get_all_packs():
            assert len(pack.scenarios) == 3
            for scenario in multiple_scenarios:
                assert scenario in pack.scenarios

    def test_add_custom_curves(self, sample_scenario):
        """Test adding scenarios to custom_curves only"""
        packer = ScenarioPacker()
        packer.add_custom_curves(sample_scenario)

        assert sample_scenario in packer._custom_curves.scenarios
        assert sample_scenario not in packer._inputs.scenarios
        assert sample_scenario not in packer._sortables.scenarios
        assert sample_scenario not in packer._output_curves.scenarios

    def test_add_inputs(self, sample_scenario):
        """Test adding scenarios to inputs only"""
        packer = ScenarioPacker()
        packer.add_inputs(sample_scenario)

        assert sample_scenario in packer._inputs.scenarios
        assert sample_scenario not in packer._custom_curves.scenarios

    def test_add_sortables(self, sample_scenario):
        """Test adding scenarios to sortables only"""
        packer = ScenarioPacker()
        packer.add_sortables(sample_scenario)

        assert sample_scenario in packer._sortables.scenarios
        assert sample_scenario not in packer._inputs.scenarios

    def test_add_output_curves(self, sample_scenario):
        """Test adding scenarios to output_curves only"""
        packer = ScenarioPacker()
        packer.add_output_curves(sample_scenario)

        assert sample_scenario in packer._output_curves.scenarios
        assert sample_scenario not in packer._inputs.scenarios


class TestScenarioPackerDataExtraction:

    def test_scenarios_empty(self):
        """Test _scenarios with empty packer"""
        packer = ScenarioPacker()
        assert len(packer._scenarios()) == 0

    def test_scenarios_single_collection(self, sample_scenario):
        """Test _scenarios with one collection"""
        packer = ScenarioPacker()
        packer.add_inputs(sample_scenario)

        all_scenarios = packer._scenarios()
        assert len(all_scenarios) == 1
        assert sample_scenario in all_scenarios

    def test_scenarios_multiple_collections(self, multiple_scenarios):
        """Test _scenarios with scenarios in different collections"""
        packer = ScenarioPacker()
        packer.add_inputs(multiple_scenarios[0])
        packer.add_custom_curves(multiple_scenarios[1])
        packer.add_sortables(multiple_scenarios[2])

        all_scenarios = packer._scenarios()
        assert len(all_scenarios) == 3
        for scenario in multiple_scenarios:
            assert scenario in all_scenarios

    def test_scenarios_overlapping_collections(self, sample_scenario):
        """Test _scenarios with same scenario in multiple collections"""
        packer = ScenarioPacker()
        packer.add_inputs(sample_scenario)
        packer.add_custom_curves(sample_scenario)

        all_scenarios = packer._scenarios()
        assert len(all_scenarios) == 1
        assert sample_scenario in all_scenarios


class TestMainInfo:

    def test_main_info_empty(self):
        """Test main_info with no scenarios"""
        packer = ScenarioPacker()
        result = packer.main_info()

        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_main_info_single_scenario(self, sample_scenario):
        """Test main_info with single scenario"""
        # Mock the to_dataframe method to return a proper DataFrame
        mock_df = pd.DataFrame(
            {sample_scenario.id: ["nl2015", 2050, "test_value"]},
            index=["area_code", "end_year", "other"],
        )

        sample_scenario.to_dataframe = Mock(return_value=mock_df)

        packer = ScenarioPacker()
        packer.add(sample_scenario)

        result = packer.main_info()

        assert not result.empty
        assert sample_scenario.id in result.columns
        assert result[sample_scenario.id]["area_code"] == "nl2015"

    def test_main_info_multiple_scenarios(self, multiple_scenarios):
        """Test main_info with multiple scenarios"""
        for i, scenario in enumerate(multiple_scenarios):
            mock_df = pd.DataFrame(
                {scenario.id: ["nl2015", 2050, f"value_{i}"]},
                index=["area_code", "end_year", "custom"],
            )
            scenario.to_dataframe = Mock(return_value=mock_df)

        packer = ScenarioPacker()
        packer.add(*multiple_scenarios)

        result = packer.main_info()

        assert len(result.columns) == 3
        for scenario in multiple_scenarios:
            assert scenario.id in result.columns


class TestInputs:

    def test_inputs_empty(self):
        """Test inputs with no scenarios"""
        packer = ScenarioPacker()
        result = packer.inputs()

        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_inputs_no_input_scenarios(self, sample_scenario):
        """Test inputs when scenario not in inputs collection"""
        packer = ScenarioPacker()
        packer.add_custom_curves(sample_scenario)

        result = packer.inputs()
        assert result.empty

    def test_inputs_single_scenario(self, scenario_with_inputs):
        """Test inputs with single scenario"""
        mock_df = pd.DataFrame(
            {"user": [1000, 2000], "unit": ["MW", "MW"], "default": [500, 800]},
            index=["wind_capacity", "solar_capacity"],
        )
        mock_df.index.name = "input"
        final_df = mock_df.set_index("unit", append=True)

        scenario_with_inputs.inputs.to_dataframe = Mock(return_value=final_df)
        scenario_with_inputs.identifier = Mock(return_value=scenario_with_inputs.id)

        packer = ScenarioPacker()
        packer.add_inputs(scenario_with_inputs)

        result = packer.inputs()

        assert not result.empty
        assert "input" in result.index.names

    def test_inputs_multiple_scenarios(self, multiple_scenarios):
        """Test inputs with multiple scenarios"""
        for i, scenario in enumerate(multiple_scenarios):
            mock_df = pd.DataFrame(
                {
                    "value": [1000 + i * 100, i * 10],
                    "unit": ["MW", "GW"],
                    "default": [500, 0],
                },
                index=["wind_capacity", f"unique_input_{i}"],
            )
            mock_df.index.name = "inputs"

            scenario.inputs.to_dataframe = Mock(
                return_value=mock_df.set_index("unit", append=True)
            )

        packer = ScenarioPacker()
        packer.add_inputs(*multiple_scenarios)

        result = packer.inputs()

        assert set(result.columns) == {s.id for s in multiple_scenarios}

        expected_keys = {
            "wind_capacity",
            "unique_input_0",
            "unique_input_1",
            "unique_input_2",
        }
        assert set(result.index) == expected_keys

        for i, s in enumerate(multiple_scenarios):
            assert result.loc["wind_capacity", s.id] == 1000 + i * 100
            assert result.loc[f"unique_input_{i}", s.id] == i * 10


class TestGqueryResults:

    def test_gquery_results_empty(self):
        """Test gquery_results with no scenarios"""
        packer = ScenarioPacker()
        result = packer.gquery_results()

        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_gquery_results_no_queries(self, sample_scenario):
        """Test gquery_results when scenarios have no queries"""
        sample_scenario.queries_requested = Mock(return_value=False)
        sample_scenario.results = Mock(return_value=pd.DataFrame())

        packer = ScenarioPacker()
        packer.add(sample_scenario)

        result = packer.gquery_results()
        assert result.empty

    def test_gquery_results_single_scenario(self, scenario_with_queries):
        """Test gquery_results with single scenario"""
        scenario_with_queries.identifier = Mock(return_value=scenario_with_queries.id)
        packer = ScenarioPacker()
        packer.add(scenario_with_queries)

        result = packer.gquery_results()

        assert not result.empty
        # Check for expected index structure (may include class name now)
        assert scenario_with_queries.id in result.columns
        assert len(result) >= 1  # Should have some data

    def test_gquery_results_multiple_scenarios(self):
        """Test gquery_results with multiple scenarios"""
        scenarios = []
        for i in range(2):
            scenario = Mock(spec=Scenario)
            scenario.id = f"query_scenario_{i}"
            scenario.area_code = "nl2015"
            scenario.end_year = 2050
            scenario.start_year = 2019
            scenario.identifier = Mock(return_value=scenario.id)

            mock_results = pd.DataFrame(
                {"future": [100 + i * 10, 200 + i * 20], "unit": ["MW", "GWh"]},
                index=[f"query_1", f"query_{i+2}"],
            )
            mock_results.index.name = "gquery"

            scenario.results = Mock(
                return_value=mock_results.set_index("unit", append=True)
            )
            scenario.queries_requested = Mock(return_value=True)
            scenarios.append(scenario)

        packer = ScenarioPacker()
        packer.add(*scenarios)

        result = packer.gquery_results()

        assert not result.empty
        for scenario in scenarios:
            assert scenario.id in result.columns


class TestDataExtractionMethods:

    def test_sortables_empty(self):
        """Test sortables with no scenarios"""
        packer = ScenarioPacker()
        result = packer.sortables()

        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_sortables_with_data(self, sample_scenario):
        """Test sortables with data"""
        mock_df = pd.DataFrame(
            {"value": [100, 200, 300]}, index=["item1", "item2", "item3"]
        )

        sample_scenario.sortables = Mock()
        sample_scenario.sortables.to_dataframe = Mock(return_value=mock_df)

        packer = ScenarioPacker()
        packer.add_sortables(sample_scenario)

        result = packer.sortables()
        assert not result.empty
        assert len(result) == 3

    def test_custom_curves_empty(self):
        """Test custom_curves with no scenarios"""
        packer = ScenarioPacker()
        result = packer.custom_curves()

        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_custom_curves_no_series(self, sample_scenario):
        """Test custom_curves when scenario returns empty series list"""
        sample_scenario.custom_curves_series = Mock(return_value=[])

        packer = ScenarioPacker()
        packer.add_custom_curves(sample_scenario)

        result = packer.custom_curves()
        assert result.empty

    def test_custom_curves_with_series(self, sample_scenario):
        """Test custom_curves with actual series data"""
        mock_series1 = pd.Series([1, 2, 3], name="curve1")
        mock_series2 = pd.Series([4, 5, 6], name="curve2")

        sample_scenario.custom_curves_series = Mock(
            return_value=[mock_series1, mock_series2]
        )

        packer = ScenarioPacker()
        packer.add_custom_curves(sample_scenario)
        result = packer.custom_curves()

        assert not result.empty
        if isinstance(result.columns, pd.MultiIndex):
            level_1 = result.columns.get_level_values(1)
            assert "curve1" in level_1
            assert "curve2" in level_1
        else:
            assert "curve1" in result.columns
            assert "curve2" in result.columns

    def test_output_curves_empty(self):
        """Test output_curves with no scenarios"""
        packer = ScenarioPacker()
        result = packer.output_curves()

        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_output_curves_with_series(self, sample_scenario):
        """Test output_curves with series data"""
        mock_series = pd.Series([10, 20, 30], name="output_curve")
        sample_scenario.all_output_curves = Mock(return_value=[mock_series])

        packer = ScenarioPacker()
        packer.add_output_curves(sample_scenario)
        result = packer.output_curves()

        assert not result.empty
        if isinstance(result.columns, pd.MultiIndex):
            assert "output_curve" in result.columns.get_level_values(1)
        else:
            assert "output_curve" in result.columns


class TestExcelExport:

    def setup_method(self):
        """Setup temp directory for Excel files"""
        self.temp_dir = tempfile.mkdtemp()

    def teardown_method(self):
        """Clean up temp files"""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_to_excel_empty_packer(self):
        """Test to_excel with empty packer raises error"""
        packer = ScenarioPacker()
        file_path = os.path.join(self.temp_dir, "empty.xlsx")

        with pytest.raises(ValueError, match="Packer was empty, nothing to export"):
            packer.to_excel(file_path)

    def test_to_excel_with_data(self, scenario_with_inputs):
        """Test to_excel with actual data"""
        dummy_main_df = pd.DataFrame(
            {"metadata": ["nl2015", 2050]}, index=["area_code", "end_year"]
        )
        dummy_inputs_df = pd.DataFrame({"value": [1000]}, index=["wind_capacity"])
        dummy_empty_df = pd.DataFrame()

        packer = ScenarioPacker()
        packer.add(scenario_with_inputs)

        scenario_with_inputs.to_dataframe = Mock(
            return_value=pd.DataFrame(
                {scenario_with_inputs.id: ["nl2015", 2050]},
                index=["area_code", "end_year"],
            )
        )

        with (
            patch.object(ScenarioPacker, "main_info", return_value=dummy_main_df),
            patch.object(
                InputsPack, "build_combined_dataframe", return_value=dummy_inputs_df
            ),
            patch.object(ScenarioPacker, "gquery_results", return_value=dummy_empty_df),
            patch.object(SortablePack, "to_dataframe", return_value=dummy_empty_df),
            patch.object(CustomCurvesPack, "to_dataframe", return_value=dummy_empty_df),
            patch.object(OutputCurvesPack, "to_dataframe", return_value=dummy_empty_df),
        ):

            file_path = os.path.join(self.temp_dir, "test_export.xlsx")
            packer.to_excel(file_path)

            # Verify file was created
            assert os.path.exists(file_path)
            assert os.path.getsize(file_path) > 0

    def test_to_excel_sheet_types(self):
        """Test to_excel with all types of data"""
        scenario = Mock(spec=Scenario)
        scenario.id = "full_scenario"
        scenario.area_code = "nl2015"
        scenario.end_year = 2050
        scenario.start_year = 2019
        scenario.identifier = Mock(return_value=scenario.id)

        # Mock all data methods to return non-empty DataFrames
        scenario.to_dataframe = Mock(
            return_value=pd.DataFrame({scenario.id: [1]}, index=["row"])
        )

        scenario.inputs = Mock()
        scenario.inputs.to_dataframe = Mock(
            return_value=pd.DataFrame({"value": [1], "unit": ["MW"]}, index=["input1"])
        )

        scenario.queries_requested = Mock(return_value=True)
        scenario.results = Mock(
            return_value=pd.DataFrame(
                {"future": [100], "unit": ["MW"]}, index=["query1"]
            )
        )

        scenario.sortables = Mock()
        scenario.sortables.to_dataframe = Mock(
            return_value=pd.DataFrame({"value": [1]}, index=["sort1"])
        )

        scenario.custom_curves_series = Mock(
            return_value=[pd.Series([1, 2], name="curve1")]
        )
        scenario.all_output_curves = Mock(
            return_value=[pd.Series([3, 4], name="carrier1")]
        )

        scenario.all_output_curves = Mock(
            return_value=[
                pd.Series([1, 2], name="curve1"),
                pd.Series([3, 4], name="carrier1"),
            ]
        )

        packer = ScenarioPacker()
        packer.add(scenario)

        file_path = os.path.join(self.temp_dir, "full_export.xlsx")
        packer.to_excel(file_path)

        assert os.path.exists(file_path)
        assert os.path.getsize(file_path) > 0


class TestUtilityMethods:

    def test_clear(self, multiple_scenarios):
        """Test clear method"""
        packer = ScenarioPacker()
        packer.add(*multiple_scenarios)

        # Verify scenarios are added
        assert len(packer._scenarios()) == 3

        packer.clear()

        # Verify all collections are empty
        assert len(packer._scenarios()) == 0
        for pack in packer._get_all_packs():
            assert len(pack.scenarios) == 0

    def test_remove_scenario(self, multiple_scenarios):
        """Test remove_scenario method"""
        packer = ScenarioPacker()
        packer.add(*multiple_scenarios)

        scenario_to_remove = multiple_scenarios[1]
        packer.remove_scenario(scenario_to_remove)

        # Verify scenario is removed from all collections
        assert scenario_to_remove not in packer._scenarios()
        assert len(packer._scenarios()) == 2

        # Verify other scenarios remain
        for scenario in [multiple_scenarios[0], multiple_scenarios[2]]:
            assert scenario in packer._scenarios()

    def test_remove_scenario_not_present(self, sample_scenario):
        """Test removing scenario that's not in packer"""
        packer = ScenarioPacker()
        other_scenario = Scenario(id="other", area_code="de", end_year=2030)

        packer.add(sample_scenario)

        # Should not raise error
        packer.remove_scenario(other_scenario)

        # Original scenario should still be there
        assert sample_scenario in packer._scenarios()

    def test_get_summary_empty(self):
        """Test get_summary with empty packer"""
        packer = ScenarioPacker()
        summary = packer.get_summary()

        assert summary["total_scenarios"] == 0
        assert summary["custom_curves"]["scenario_count"] == 0
        assert summary["inputs"]["scenario_count"] == 0
        assert summary["sortables"]["scenario_count"] == 0
        assert summary["output_curves"]["scenario_count"] == 0
        assert summary["scenario_ids"] == []

    def test_get_summary_with_data(self, multiple_scenarios):
        """Test get_summary with scenarios"""
        packer = ScenarioPacker()
        packer.add_inputs(multiple_scenarios[0])
        packer.add_custom_curves(multiple_scenarios[1])
        packer.add(multiple_scenarios[2])

        summary = packer.get_summary()

        assert summary["total_scenarios"] == 3
        assert summary["inputs"]["scenario_count"] == 2  # scenarios 0 and 2
        assert summary["custom_curves"]["scenario_count"] == 2  # scenarios 1 and 2
        assert summary["sortables"]["scenario_count"] == 1  # scenario 2 only
        assert summary["output_curves"]["scenario_count"] == 1  # scenario 2 only
        assert len(summary["scenario_ids"]) == 3
        assert all(s.id in summary["scenario_ids"] for s in multiple_scenarios)


class TestFromExcel:
    def test_from_excel(self):
        ScenarioPacker.from_excel("tests/fixtures/my_input_excel.xlsx")


class TestExportConfigResolver:
    """Test the ExportConfigResolver class"""

    def test_resolve_boolean_explicit_value(self):
        """Test resolve_boolean with explicit value provided"""
        assert ExportConfigResolver.resolve_boolean(True, False, False) == True
        assert ExportConfigResolver.resolve_boolean(False, True, True) == False
        assert ExportConfigResolver.resolve_boolean(None, True, False) == True
        assert ExportConfigResolver.resolve_boolean(None, None, True) == True

    def test_parse_config_from_series(self):
        """Test parsing config from pandas Series"""
        series = pd.Series(
            {
                "inputs": "yes",
                "sortables": "no",
                "defaults": "1",
                "min_max": "0",
                "exports": "electricity,gas",
            }
        )

        config = ExportConfigResolver._parse_config_from_series(series)

        assert config.include_inputs == True
        assert config.include_sortables == False
        assert config.inputs_defaults == True
        assert config.inputs_min_max == False
        assert config.output_carriers == ["electricity", "gas"]


class TestScenarioPackerHelpers:

    def test_find_first_non_empty_row(self):
        """Test _find_first_non_empty_row method"""
        packer = ScenarioPacker()

        assert packer._find_first_non_empty_row(None) is None

        empty = pd.DataFrame([[float("nan")], [float("nan")]])
        assert packer._find_first_non_empty_row(empty) is None

        # Test with actual data
        df = pd.DataFrame([[None, None], ["header", "value"], [1, 2]])
        assert packer._find_first_non_empty_row(df) == 1

    def test_is_helper_column(self):
        """Test _is_helper_column method"""
        packer = ScenarioPacker()
        helpers = {"sortables", "hour", "index"}

        assert packer._is_helper_column(123, helpers) is True
        assert packer._is_helper_column(" ", helpers) is True
        assert packer._is_helper_column("NaN", helpers) is True
        assert packer._is_helper_column("hour", helpers) is True
        assert packer._is_helper_column("value", helpers) is False

    def test_normalize_sheet(self):
        """Test _normalize_sheet method"""
        packer = ScenarioPacker()

        # None -> empty
        assert packer._normalize_sheet(None, helper_names=set()).empty

        # Build a frame with header at row 1 (0-based)
        raw = pd.DataFrame(
            [
                [None, None, None],
                ["index", "heat_network", "value"],  # header
                [1, "hn", 10],
                [2, "hn", 20],
            ]
        )

        norm = packer._normalize_sheet(
            raw,
            helper_names={"index"},
            reset_index=False,
            rename_map={"heat_network": "heat_network_lt"},
        )

        # index column removed, rename applied, index preserved
        assert list(norm.columns) == ["heat_network_lt", "value"]
        assert norm.index.tolist() == [2, 3]  # original DataFrame indices kept

    def test_safe_get_bool(self):
        """Test _safe_get_bool method"""
        packer = ScenarioPacker()
        na = float("nan")
        assert packer._safe_get_bool(None) is None
        assert packer._safe_get_bool(na) is None
        assert packer._safe_get_bool(True) is True
        assert packer._safe_get_bool(False) is False
        assert packer._safe_get_bool(1) is True
        assert packer._safe_get_bool(0.0) is False
        assert packer._safe_get_bool("yes") is True
        assert packer._safe_get_bool("No") is False
        assert packer._safe_get_bool("1") is True
        assert packer._safe_get_bool("maybe") is None

    def test_safe_get_int(self):
        """Test _safe_get_int method"""
        packer = ScenarioPacker()
        na = float("nan")
        assert packer._safe_get_int(None) is None
        assert packer._safe_get_int(na) is None
        assert packer._safe_get_int(5) == 5
        assert packer._safe_get_int(5.9) == 5
        assert packer._safe_get_int("7") == 7
        assert packer._safe_get_int("abc") is None

    def test_load_or_create_scenario_load_new_and_failures(self, monkeypatch):
        """Test _load_or_create_scenario method"""
        packer = ScenarioPacker()

        loaded = Mock(spec=Scenario)
        created = Mock(spec=Scenario)

        # Successful load
        monkeypatch.setattr(Scenario, "load", staticmethod(lambda sid: loaded))
        out = packer._load_or_create_scenario(42, "nl2015", 2050, "COL")
        assert out is loaded

        # Failing load -> None
        def boom(_):
            raise RuntimeError("bad")

        monkeypatch.setattr(Scenario, "load", staticmethod(boom))
        assert packer._load_or_create_scenario(42, "nl2015", 2050, "COL") is None

        # Successful new
        monkeypatch.setattr(Scenario, "new", staticmethod(lambda a, y: created))
        out = packer._load_or_create_scenario(None, "de", 2030, "COL2")
        assert out is created

        # Failing new -> None
        def boom2(_, __):
            raise ValueError("bad")

        monkeypatch.setattr(Scenario, "new", staticmethod(boom2))
        assert packer._load_or_create_scenario(None, "nl", 2050, "C") is None

        # Missing fields -> None
        assert packer._load_or_create_scenario(None, None, None, "C") is None

    def test_extract_metadata_updates_and_apply(self):
        """Test metadata extraction and application"""
        packer = ScenarioPacker()

        # Test extraction
        series = pd.Series(
            {"private": True, "template": 7, "source": " src ", "title": "  title  "}
        )

        meta = packer._extract_metadata_updates(series)
        assert meta == {
            "private": True,
            "template": 7,
            "source": "src",
            "title": "title",
        }

        # empty strings trimmed out
        series_empty = pd.Series(
            {"private": None, "template": None, "source": " ", "title": ""}
        )
        meta_empty = packer._extract_metadata_updates(series_empty)
        assert meta_empty == {}

        # apply updates
        scenario = Mock(spec=Scenario)
        packer._apply_metadata_to_scenario(scenario, {"private": False})
        scenario.update_metadata.assert_called_once_with(private=False)

        # swallow exceptions
        scenario.update_metadata.side_effect = RuntimeError("boom")
        packer._apply_metadata_to_scenario(
            scenario, {"private": True}
        )  # should not raise

        # no updates does nothing
        scenario.update_metadata.reset_mock()
        scenario.update_metadata.side_effect = None
        packer._apply_metadata_to_scenario(scenario, {})
        scenario.update_metadata.assert_not_called()

    def test_extract_scenario_sheet_info_series_and_df(self):
        """Test _extract_scenario_sheet_info method"""
        packer = ScenarioPacker()

        ser = pd.Series(
            {
                "short_name": "S",
                "sortables": "SORT1",
                "custom_curves": "CUR1",
            },
            name="COL1",
        )
        out = packer._extract_scenario_sheet_info(ser)
        assert out == {
            "COL1": {"short_name": "S", "sortables": "SORT1", "custom_curves": "CUR1"}
        }

        df = pd.DataFrame(
            {
                "A": {"short_name": None, "sortables": "S_A", "custom_curves": None},
                "B": {"short_name": "B_S", "sortables": None, "custom_curves": "C_B"},
            }
        )
        out2 = packer._extract_scenario_sheet_info(df)
        assert out2["A"]["short_name"] == "A"
        assert out2["A"]["sortables"] == "S_A"
        assert out2["A"]["custom_curves"] is None
        assert out2["B"]["short_name"] == "B_S"
        assert out2["B"]["custom_curves"] == "C_B"

    def test_process_single_scenario_sortables(self):
        """Test _process_single_scenario_sortables method"""
        packer = ScenarioPacker()
        scenario = Mock(spec=Scenario)

        # Build a sheet where header row contains helpers + target column to be renamed
        raw = pd.DataFrame(
            [
                [None, None, None],
                ["sortables", "heat_network", "hour"],
                [None, "lt", 1],
                [None, "mt", 2],
            ]
        )

        packer._process_single_scenario_sortables(scenario, raw)
        assert scenario.set_sortables_from_dataframe.called
        df_arg = scenario.set_sortables_from_dataframe.call_args[0][0]
        assert "heat_network_lt" in df_arg.columns
        assert "hour" not in df_arg.columns

    def test_process_single_scenario_sortables_empty_after_normalize(self):
        """Test _process_single_scenario_sortables with empty data after normalization"""
        packer = ScenarioPacker()
        scenario = Mock(spec=Scenario)

        raw = pd.DataFrame(
            [
                [None, None],
                ["sortables", "hour"],
                [None, 1],
            ]
        )

        packer._process_single_scenario_sortables(scenario, raw)
        scenario.set_sortables_from_dataframe.assert_not_called()

    def test_process_single_scenario_curves_success_and_error(self, monkeypatch):
        """Test _process_single_scenario_curves method"""
        packer = ScenarioPacker()
        scenario = Mock(spec=Scenario)
        scenario.id = 999

        raw = pd.DataFrame(
            [
                [None, None],
                ["custom_curves", "value"],
                ["curve_1", 1.0],
                ["curve_2", 2.0],
            ]
        )

        dummy_curves = Mock(spec=CustomCurves)
        monkeypatch.setattr(
            CustomCurves,
            "_from_dataframe",
            staticmethod(lambda df, scenario_id: dummy_curves),
        )
        packer._process_single_scenario_curves(scenario, raw)
        scenario.update_custom_curves.assert_called_once_with(dummy_curves)
        scenario.update_custom_curves.reset_mock()

        def boom(_df, scenario_id):
            raise RuntimeError("bad curves")

        monkeypatch.setattr(CustomCurves, "_from_dataframe", staticmethod(boom))
        packer._process_single_scenario_curves(scenario, raw)
        scenario.update_custom_curves.assert_not_called()

    def test_process_single_scenario_curves_empty_after_normalize(self):
        """Test _process_single_scenario_curves with empty data after normalization"""
        packer = ScenarioPacker()
        scenario = Mock(spec=Scenario)
        scenario.id = 1

        raw = pd.DataFrame(
            [
                [None],
                ["custom_curves"],
                [None],
            ]
        )
        packer._process_single_scenario_curves(scenario, raw)
        scenario.update_custom_curves.assert_not_called()


class TestCreateScenarioFromColumn:

    def test_create_scenario_from_column_loads_and_updates(self, monkeypatch):
        """Test _create_scenario_from_column method with loading existing scenario"""
        packer = ScenarioPacker()
        scenario = Mock(spec=Scenario)
        scenario.identifier = Mock(return_value="SID")
        monkeypatch.setattr(Scenario, "load", staticmethod(lambda sid: scenario))

        ser = pd.Series(
            {
                "scenario_id": "101",
                "area_code": "nl2015",
                "end_year": 2050,
                "private": "yes",
                "template": "7",
                "source": " src ",
                "title": " title ",
            }
        )

        out = packer._create_scenario_from_column("COL", ser)
        assert out is scenario
        scenario.update_metadata.assert_called_once()

    def test_create_scenario_from_column_creates(self, monkeypatch):
        """Test _create_scenario_from_column method with creating new scenario"""
        packer = ScenarioPacker()
        scenario = Mock(spec=Scenario)
        scenario.identifier = Mock(return_value="NEW")
        monkeypatch.setattr(Scenario, "new", staticmethod(lambda a, y: scenario))

        ser = pd.Series(
            {
                "scenario_id": None,
                "area_code": "de",
                "end_year": 2030,
                "private": 0,
                "template": None,
            }
        )

        out = packer._create_scenario_from_column("COL", ser)
        assert out is scenario

    def test_create_scenario_from_column_returns_none_on_fail(self, monkeypatch):
        """Test _create_scenario_from_column returns None on failure"""
        packer = ScenarioPacker()
        monkeypatch.setattr(
            ScenarioPacker, "_load_or_create_scenario", lambda self, *a, **k: None
        )
        ser = pd.Series({"scenario_id": None, "area_code": None, "end_year": None})
        assert packer._create_scenario_from_column("COL", ser) is None


class TestFromExcelDetailed:

    def test_from_excel_full_flow(self, tmp_path, monkeypatch):
        """Test complete from_excel flow"""
        # Prepare MAIN with two scenarios: one load, one create
        main = pd.DataFrame(
            {
                "S1": {
                    "scenario_id": 101,
                    "area_code": "nl2015",
                    "end_year": 2050,
                    "private": "yes",
                    "template": 3,
                    "source": "source1",
                    "title": "Title 1",
                    "short_name": "Short1",
                    "sortables": "S1_SORT",
                    "custom_curves": "S1_CURVES",
                },
                "S2": {
                    "scenario_id": None,
                    "area_code": "de",
                    "end_year": 2030,
                    "private": 0,
                    "template": None,
                    "source": "",
                    "title": "",
                    "short_name": None,
                    "sortables": "S2_SORT",
                    "custom_curves": None,
                },
            }
        )

        # Other sheets
        params = pd.DataFrame([["helper", "value"], ["input_key", 1]])
        gqueries = pd.DataFrame([["gquery", "future"], ["co2_emissions", 100]])
        s1_sort = pd.DataFrame([[None, None], ["sortables", "value"], ["a", 1]])
        s2_sort = pd.DataFrame([[None, None], ["sortables", "value"], ["b", 2]])
        s1_curves = pd.DataFrame([[None, None], ["custom_curves", "value"], ["x", 1]])

        path = tmp_path / "import.xlsx"
        with pd.ExcelWriter(path, engine="xlsxwriter") as writer:
            main.to_excel(writer, sheet_name="MAIN")
            params.to_excel(
                writer, sheet_name="SLIDER_SETTINGS", header=False, index=False
            )
            gqueries.to_excel(writer, sheet_name="GQUERIES", header=False, index=False)
            s1_sort.to_excel(writer, sheet_name="S1_SORT", header=False, index=False)
            s2_sort.to_excel(writer, sheet_name="S2_SORT", header=False, index=False)
            s1_curves.to_excel(
                writer, sheet_name="S1_CURVES", header=False, index=False
            )

        # Patch loading/creating and pack interactions
        s_loaded = Mock(spec=Scenario)
        s_loaded.id = "101"
        s_loaded.identifier = Mock(return_value="101")
        s_created = Mock(spec=Scenario)
        s_created.id = "created"
        s_created.identifier = Mock(return_value="created")

        monkeypatch.setattr(Scenario, "load", staticmethod(lambda sid: s_loaded))
        monkeypatch.setattr(Scenario, "new", staticmethod(lambda a, y: s_created))

        # Spy on inputs and queries imports
        with (
            patch.object(InputsPack, "set_scenario_short_names") as set_sn,
            patch.object(InputsPack, "from_dataframe") as from_df,
            patch.object(QueryPack, "from_dataframe") as gq_from_df,
            patch.object(
                ScenarioPacker, "_process_single_scenario_sortables"
            ) as proc_sort,
            patch.object(
                ScenarioPacker, "_process_single_scenario_curves"
            ) as proc_curves,
        ):
            packer = ScenarioPacker.from_excel(str(path))

            assert isinstance(packer, ScenarioPacker)
            assert s_loaded in packer._scenarios()
            assert s_created in packer._scenarios()

            set_sn.assert_called_once()
            from_df.assert_called_once()
            gq_from_df.assert_called_once()

            # Called once for each scenario with a sortables sheet
            assert proc_sort.call_count == 2
            proc_curves.assert_called_once()

    def test_from_excel_missing_or_bad_main(self, tmp_path):
        """Test from_excel with missing or bad main sheet"""
        packer = ScenarioPacker.from_excel(str(tmp_path / "bad.xlsx"))
        assert isinstance(packer, ScenarioPacker)
        assert len(packer._scenarios()) == 0

        # File with no MAIN sheet
        path = tmp_path / "no_main.xlsx"
        with pd.ExcelWriter(path, engine="xlsxwriter") as writer:
            pd.DataFrame([[1]]).to_excel(writer, sheet_name="OTHER")
        packer2 = ScenarioPacker.from_excel(str(path))
        assert isinstance(packer2, ScenarioPacker)
        assert len(packer2._scenarios()) == 0

        # File with empty MAIN sheet
        path2 = tmp_path / "empty_main.xlsx"
        with pd.ExcelWriter(path2, engine="xlsxwriter") as writer:
            pd.DataFrame().to_excel(writer, sheet_name="MAIN")
        packer3 = ScenarioPacker.from_excel(str(path2))
        assert isinstance(packer3, ScenarioPacker)
        assert len(packer3._scenarios()) == 0

    def test_from_excel_slider_settings_and_gqueries_errors(
        self, tmp_path, monkeypatch
    ):
        """Test from_excel with errors in slider settings and gqueries import"""
        main = pd.DataFrame(
            {
                "S": {
                    "scenario_id": None,
                    "area_code": "nl2015",
                    "end_year": 2050,
                    "sortables": None,
                    "custom_curves": None,
                }
            }
        )
        params = pd.DataFrame([["helper", "value"], ["input_key", 1]])
        gqueries = pd.DataFrame([["gquery", "future"], ["co2_emissions", 100]])

        path = tmp_path / "errs.xlsx"
        with pd.ExcelWriter(path, engine="xlsxwriter") as writer:
            main.to_excel(writer, sheet_name="MAIN")
            params.to_excel(
                writer, sheet_name="SLIDER_SETTINGS", header=False, index=False
            )
            gqueries.to_excel(writer, sheet_name="GQUERIES", header=False, index=False)

        # Create returns a simple scenario
        s_created = Mock(spec=Scenario)
        s_created.id = "created"
        s_created.identifier = Mock(return_value="created")
        monkeypatch.setattr(Scenario, "new", staticmethod(lambda a, y: s_created))

        with (
            patch.object(InputsPack, "set_scenario_short_names") as set_sn,
            patch.object(
                InputsPack, "from_dataframe", side_effect=RuntimeError("bad params")
            ),
            patch.object(
                QueryPack, "from_dataframe", side_effect=RuntimeError("bad gq")
            ),
        ):
            packer = ScenarioPacker.from_excel(str(path))

            assert isinstance(packer, ScenarioPacker)
            # Scenario was still created even if imports failed
            assert s_created in packer._scenarios()
            set_sn.assert_called_once()

    def test_from_excel_gqueries_sheet_name_fallback(self, tmp_path, monkeypatch):
        """Test from_excel with gqueries sheet name fallback"""
        main = pd.DataFrame(
            {"S": {"scenario_id": None, "area_code": "nl2015", "end_year": 2050}}
        )

        path = tmp_path / "gq_fallback.xlsx"
        with pd.ExcelWriter(path, engine="xlsxwriter") as writer:
            main.to_excel(writer, sheet_name="MAIN")
            pd.DataFrame([["gquery"], ["total_costs"]]).to_excel(
                writer, sheet_name="GQ2", header=False, index=False
            )

        s_created = Mock(spec=Scenario)
        s_created.id = "created"
        s_created.identifier = Mock(return_value="created")
        monkeypatch.setattr(Scenario, "new", staticmethod(lambda a, y: s_created))

        with patch.object(QueryPack, "sheet_name", "GQ2"):
            with patch.object(QueryPack, "from_dataframe") as gq_from_df:
                packer = ScenarioPacker.from_excel(str(path))
                assert s_created in packer._scenarios()
                gq_from_df.assert_called_once()

    def test_from_excel_processing_sortables_and_curves_errors(
        self, tmp_path, monkeypatch
    ):
        """Test from_excel with errors in processing sortables and curves"""
        main = pd.DataFrame(
            {
                "S": {
                    "scenario_id": None,
                    "area_code": "nl2015",
                    "end_year": 2050,
                    "sortables": "S_SORT",
                    "custom_curves": "S_CURVES",
                }
            }
        )

        path = tmp_path / "proc_errs.xlsx"
        with pd.ExcelWriter(path, engine="xlsxwriter") as writer:
            main.to_excel(writer, sheet_name="MAIN")
            pd.DataFrame([[None], ["sortables"], ["a"]]).to_excel(
                writer, sheet_name="S_SORT", header=False, index=False
            )
            pd.DataFrame([[None], ["custom_curves"], ["x"]]).to_excel(
                writer, sheet_name="S_CURVES", header=False, index=False
            )

        s_created = Mock(spec=Scenario)
        s_created.id = "created"
        s_created.identifier = Mock(return_value="created")
        monkeypatch.setattr(Scenario, "new", staticmethod(lambda a, y: s_created))

        with (
            patch.object(
                ScenarioPacker,
                "_process_single_scenario_sortables",
                side_effect=RuntimeError("bad sort"),
            ),
            patch.object(
                ScenarioPacker,
                "_process_single_scenario_curves",
                side_effect=RuntimeError("bad cur"),
            ),
        ):
            packer = ScenarioPacker.from_excel(str(path))
            assert isinstance(packer, ScenarioPacker)
            assert s_created in packer._scenarios()

    def test_from_excel_setup_column_exception_and_all_fail(
        self, tmp_path, monkeypatch
    ):
        """Test from_excel with setup column exceptions"""
        # Two columns: first raises, second returns scenario
        main = pd.DataFrame(
            {
                "A": {"scenario_id": None, "area_code": "nl2015", "end_year": 2050},
                "B": {"scenario_id": None, "area_code": "de", "end_year": 2030},
            }
        )
        path = tmp_path / "columns_mix.xlsx"
        with pd.ExcelWriter(path, engine="xlsxwriter") as writer:
            main.to_excel(writer, sheet_name="MAIN")

        # Patch method to raise for A and create for B
        def setup(col_name, col_ser):
            if col_name == "A":
                raise RuntimeError("boom")
            s = Mock(spec=Scenario)
            s.id = "BID"
            s.identifier = Mock(return_value="BID")
            return s

        with patch.object(
            ScenarioPacker, "_create_scenario_from_column", side_effect=setup
        ):
            packer = ScenarioPacker.from_excel(str(path))
            assert any(s.id == "BID" for s in packer._scenarios())

        # All columns fail -> 0 scenarios, early return
        with patch.object(
            ScenarioPacker,
            "_create_scenario_from_column",
            side_effect=RuntimeError("e"),
        ):
            packer2 = ScenarioPacker.from_excel(str(path))
            assert len(packer2._scenarios()) == 0

    def test_from_excel_missing_slider_settings_sheet_parse_error(
        self, tmp_path, monkeypatch
    ):
        """Test from_excel with missing slider settings sheet"""
        main = pd.DataFrame(
            {"S": {"scenario_id": None, "area_code": "nl2015", "end_year": 2050}}
        )
        path = tmp_path / "no_params.xlsx"
        with pd.ExcelWriter(path, engine="xlsxwriter") as writer:
            main.to_excel(writer, sheet_name="MAIN")

        s_created = Mock(spec=Scenario)
        s_created.id = "created"
        s_created.identifier = Mock(return_value="created")
        monkeypatch.setattr(Scenario, "new", staticmethod(lambda a, y: s_created))
        packer = ScenarioPacker.from_excel(str(path))
        assert s_created in packer._scenarios()

    def test_from_excel_gqueries_parse_raises(self, tmp_path, monkeypatch):
        """Test from_excel with gqueries parse error"""
        main = pd.DataFrame(
            {"S": {"scenario_id": None, "area_code": "nl2015", "end_year": 2050}}
        )
        path = tmp_path / "gq_parse_err.xlsx"
        with pd.ExcelWriter(path, engine="xlsxwriter") as writer:
            main.to_excel(writer, sheet_name="MAIN")
            pd.DataFrame([["gquery"], ["total_costs"]]).to_excel(
                writer, sheet_name="GQUERIES", header=False, index=False
            )

        s_created = Mock(spec=Scenario)
        s_created.id = "created"
        s_created.identifier = Mock(return_value="created")
        monkeypatch.setattr(Scenario, "new", staticmethod(lambda a, y: s_created))

        original_parse = pd.ExcelFile.parse

        def parse_proxy(self, sheet_name, *a, **k):
            if sheet_name == "GQUERIES":
                raise ValueError("bad parse")
            return original_parse(self, sheet_name, *a, **k)

        with patch.object(pd.ExcelFile, "parse", parse_proxy):
            packer = ScenarioPacker.from_excel(str(path))
            assert s_created in packer._scenarios()


class TestInputsPackIntegration:
    """Test integration with the new InputsPack.build_combined_dataframe method"""

    def test_inputs_pack_build_combined_dataframe_called(self, sample_scenario):
        """Test that to_excel calls the new build_combined_dataframe method"""
        packer = ScenarioPacker()
        packer.add(sample_scenario)

        sample_scenario.to_dataframe = Mock(
            return_value=pd.DataFrame({sample_scenario.id: ["test"]}, index=["row"])
        )

        with (
            patch.object(InputsPack, "build_combined_dataframe") as mock_build,
            patch("xlsxwriter.Workbook") as mock_workbook_class,
        ):
            mock_build.return_value = pd.DataFrame({"test": [1]}, index=["input1"])
            mock_workbook = Mock()
            mock_workbook_class.return_value = mock_workbook

            packer.to_excel("test.xlsx", include_inputs=True)

            # Verify the new method was called with correct parameters
            mock_build.assert_called_once_with(
                include_defaults=False, include_min_max=False
            )

    def test_inputs_pack_build_combined_dataframe_with_flags(self, sample_scenario):
        """Test that flags are passed correctly to build_combined_dataframe"""
        packer = ScenarioPacker()
        packer.add(sample_scenario)

        sample_scenario.to_dataframe = Mock(
            return_value=pd.DataFrame({sample_scenario.id: ["test"]}, index=["row"])
        )

        # Mock a global config that sets inputs defaults and min_max
        mock_config = Mock()
        mock_config.inputs_defaults = True
        mock_config.inputs_min_max = True
        mock_config.include_inputs = True
        mock_config.include_sortables = False
        mock_config.include_custom_curves = False
        mock_config.include_gqueries = False
        mock_config.output_carriers = None

        with (
            patch.object(InputsPack, "build_combined_dataframe") as mock_build,
            patch.object(
                ScenarioPacker, "_get_global_export_config", return_value=mock_config
            ),
            patch("xlsxwriter.Workbook") as mock_workbook_class,
        ):
            mock_build.return_value = pd.DataFrame({"test": [1]}, index=["input1"])
            mock_workbook = Mock()
            mock_workbook_class.return_value = mock_workbook

            packer.to_excel("test.xlsx")

            # Verify the method was called with the config flags
            mock_build.assert_called_once_with(
                include_defaults=True, include_min_max=True
            )
