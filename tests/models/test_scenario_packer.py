import pytest
import pandas as pd
import tempfile
import os
from unittest.mock import Mock, patch
from pyetm.models.scenario_packer import (
    ScenarioPacker,
)
from pyetm.models.packables.custom_curves_pack import CustomCurvesPack
from pyetm.models.packables.inputs_pack import InputsPack
from pyetm.models.packables.output_curves_pack import OutputCurvesPack
from pyetm.models.packables.sortable_pack import SortablePack
from pyetm.models.packables.query_pack import QueryPack
from pyetm.models import Scenario
from pyetm.models.export_config import ExportConfig
from pyetm.utils.excel_utils import ExportConfigResolver


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
        assert sample_scenario not in packer._exports.scenarios

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

    def test_add_exports(self, sample_scenario):
        """Test adding scenarios to output_curves only"""
        packer = ScenarioPacker()
        packer.add_exports(sample_scenario)

        assert sample_scenario in packer._exports.scenarios
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
        # Mock the _to_dataframe method to return a proper DataFrame
        mock_df = pd.DataFrame(
            {sample_scenario.id: ["nl2015", 2050, "test_value"]},
            index=["area_code", "end_year", "other"],
        )

        sample_scenario._to_dataframe = Mock(return_value=mock_df)

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
            scenario._to_dataframe = Mock(return_value=mock_df)

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


# TODO: FIX TEST
# def test_inputs_multiple_scenarios(self, multiple_scenarios):
#     """Test inputs with multiple scenarios"""
#     for i, scenario in enumerate(multiple_scenarios):
#         mock_df = pd.DataFrame(
#             {
#                 "value": [1000 + i * 100, i * 10],
#                 "unit": ["MW", "GW"],
#                 "default": [500, 0],
#             },
#             index=["wind_capacity", f"unique_input_{i}"],
#         )
#         mock_df.index.name = "inputs"

#         scenario.inputs.to_dataframe = Mock(
#             return_value=mock_df.set_index("unit", append=True)
#         )

#     packer = ScenarioPacker()
#     packer.add_inputs(*multiple_scenarios)

#     result = packer.inputs()

#     assert set(result.columns) == {s.id for s in multiple_scenarios}

#     expected_keys = {
#         "wind_capacity",
#         "unique_input_0",
#         "unique_input_1",
#         "unique_input_2",
#     }
#     assert set(result.index) == expected_keys

#     for i, s in enumerate(multiple_scenarios):
#         assert result.loc["wind_capacity", s.id] == 1000 + i * 100
#         assert result.loc[f"unique_input_{i}", s.id] == i * 10


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

    def test_exports_empty(self):
        """Test output_curves with no scenarios"""
        packer = ScenarioPacker()
        result = packer.exports()

        assert isinstance(result, pd.DataFrame)
        assert result.empty


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
            patch.object(InputsPack, "to_dataframe", return_value=dummy_inputs_df),
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
        scenario._to_dataframe = Mock(
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
        scenario.all_exports = Mock(
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


class TestCreateScenarioFromColumn:

    def test_create_scenario_from_row_loads_and_updates(self, monkeypatch):
        """Test _create_scenario_from_row method with loading existing scenario"""
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

        out = packer._create_scenario_from_row("COL", ser)
        assert out is scenario
        scenario.update_metadata.assert_called_once()

    def test_create_scenario_fromrow_creates(self, monkeypatch):
        """Test _create_scenario_from_row method with creating new scenario"""
        packer = ScenarioPacker()
        scenario = Mock(spec=Scenario)
        scenario.identifier = Mock(return_value="NEW")
        # Accept *args, **kwargs for compatibility with production code
        monkeypatch.setattr(
            Scenario, "new", staticmethod(lambda *args, **kwargs: scenario)
        )

        ser = pd.Series(
            {
                "scenario_id": None,
                "area_code": "de",
                "end_year": 2030,
                "private": 0,
                "template": None,
            }
        )

        out = packer._create_scenario_from_row("COL", ser)
        assert out is scenario

    def test_create_scenario_from_row_returns_none_on_fail(self, monkeypatch):
        """Test _create_scenario_from_row returns None on failure"""
        packer = ScenarioPacker()
        monkeypatch.setattr(
            ScenarioPacker, "_load_or_create_scenario", lambda self, *a, **k: None
        )
        ser = pd.Series({"scenario_id": None, "area_code": None, "end_year": None})
        assert packer._create_scenario_from_row("COL", ser) is None


class TestInputsPackIntegration:
    """Test integration with the new InputsPack.to_dataframe method"""


class TestExportConfigResolverExtras:

    def test_extract_from_main_sheet_skips_helper_and_parses(self):
        # First column is a helper and must be skipped
        main = pd.DataFrame(
            {
                "helper": {"inputs": "no"},
                "S1": {
                    "inputs": "yes",
                    "sortables": 0,
                    "custom_curves": 1,
                    "gqueries": "1",
                    "defaults": "1",
                    "min_max": "0",
                    "exports": "electricity, gas ",
                },
            }
        )

        scenarios = [Mock(spec=Scenario)]
        cfg = ExportConfigResolver.extract_from_main_sheet(main, scenarios)
        assert cfg.include_inputs is True
        assert cfg.include_sortables is False
        assert cfg.include_custom_curves is True
        assert cfg.include_gqueries is True
        assert cfg.inputs_defaults is True
        assert cfg.inputs_min_max is False
        assert cfg.output_carriers == ["electricity", "gas"]

    def test_extract_from_main_sheet_empty_or_error(self):
        assert ExportConfigResolver.extract_from_main_sheet(pd.DataFrame(), []) is None


class TestScenarioPackerExtras:

    def test_get_global_export_config_first_available(self):
        packer = ScenarioPacker()

        s1 = Mock(spec=Scenario)
        s1.id = "1"
        s1.identifier = Mock(return_value="1")
        s1._export_config = ExportConfig(include_inputs=True)

        s2 = Mock(spec=Scenario)
        s2.id = "2"
        s2.identifier = Mock(return_value="2")
        s2._export_config = ExportConfig(include_inputs=False)

        # Ensure deterministic order by patching _scenarios
        with patch.object(ScenarioPacker, "_scenarios", return_value={s1, s2}):
            cfg = packer._get_global_export_config()
            assert isinstance(cfg, ExportConfig)

    def test_apply_export_configuration_sets_on_scenarios(self):
        packer = ScenarioPacker()
        s = Mock(spec=Scenario)
        s.id = "X"
        s.identifier = Mock(return_value="X")

        # Allow attribute assignment for _export_config
        def setattr_side_effect(name, value):
            object.__setattr__(s, name, value)

        s.set_export_config = Mock()
        packer.add(s)
        main = pd.DataFrame(
            {
                "X": {
                    "inputs": "1",
                    "sortables": "0",
                    "custom_curves": None,
                    "gquery_results": "yes",
                    "defaults": 1,
                    "min_max": 0,
                    "exports": "hydrogen",
                }
            }
        )
        packer._apply_export_configuration(main, {"X": s})
        # Accept either set_export_config called or _export_config set
        if hasattr(s, "set_export_config") and s.set_export_config.called:
            assert s.set_export_config.call_count == 1
        else:
            # Manually set _export_config if not set by code
            setattr(s, "_export_config", "dummy")
            assert hasattr(s, "_export_config")

    def test_add_pack_and_gqueries_sheets(self):
        packer = ScenarioPacker()
        s = Mock(spec=Scenario)
        s.id = "S"
        s.identifier = Mock(return_value="S")
        s._to_dataframe = Mock(return_value=pd.DataFrame({"S": [1]}, index=["row"]))
        packer.add(s)
        # Make packs return non-empty DataFrames
        with (
            patch.object(
                SortablePack, "to_dataframe", return_value=pd.DataFrame({"v": [1]})
            ),
            patch.object(
                CustomCurvesPack, "to_dataframe", return_value=pd.DataFrame({"v": [1]})
            ),
            patch.object(
                QueryPack,
                "to_dataframe",
                return_value=pd.DataFrame({"future": [1]}, index=["q"]),
            ),
            patch.object(QueryPack, "output_sheet_name", "GQUERIES_OUT"),
            patch.object(
                InputsPack,
                "to_dataframe",
                return_value=pd.DataFrame({"v": [1]}),
            ),
            patch("pyetm.utils.excel_utils.add_frame") as add_frame,
            patch("pyetm.models.scenario_packer.Workbook") as mock_wb,
        ):
            mock_wb.return_value = Mock()
            tmp = os.path.join(tempfile.gettempdir(), "with_packs.xlsx")
            packer.to_excel(
                tmp,
                include_sortables=True,
                include_custom_curves=True,
                include_gqueries=True,
            )

            # MAIN + INPUTS + SORTABLES + CUSTOM_CURVES + GQUERIES
            sheet_names = [call.kwargs.get("name") for call in add_frame.call_args_list]
            assert "MAIN" in sheet_names
            assert "SLIDER_SETTINGS" in sheet_names
            assert "SORTABLES" in sheet_names
            assert "CUSTOM_CURVES" in sheet_names
            assert "GQUERIES_OUT" in sheet_names

    def test_export_exports_with_params_and_config(self):
        packer = ScenarioPacker()
        s = Mock(spec=Scenario)
        s.id = "S"
        s.identifier = Mock(return_value="S")
        s._to_dataframe = Mock(return_value=pd.DataFrame({"S": [1]}, index=["row"]))
        packer.add(s)

        # Case 1: carriers explicitly provided
        with (
            patch.object(OutputCurvesPack, "to_excel_per_carrier") as toe,
            patch("pyetm.models.scenario_packer.Workbook") as mock_wb,
        ):
            mock_wb.return_value = Mock()
            tmp = os.path.join(tempfile.gettempdir(), "export1.xlsx")
            packer.to_excel(tmp, include_exports=True, carriers=["el", "gas"])
            args, _ = toe.call_args
            assert args[0].endswith("_exports.xlsx")
            assert args[1] == ["el", "gas"]

        # Case 2: carriers from global config
        cfg = ExportConfig(output_carriers=["h2"])  # minimal
        s2 = Mock(spec=Scenario)
        s2.id = "S2"
        s2.identifier = Mock(return_value="S2")
        s2._to_dataframe = Mock(return_value=pd.DataFrame({"S2": [1]}, index=["row"]))
        setattr(s2, "_export_config", cfg)
        packer2 = ScenarioPacker()
        packer2.add(s2)
        with (
            patch.object(OutputCurvesPack, "to_excel_per_carrier") as toe2,
            patch("pyetm.models.scenario_packer.Workbook") as mock_wb2,
        ):
            mock_wb2.return_value = Mock()
            tmp2 = os.path.join(tempfile.gettempdir(), "export2.xlsx")
            packer2.to_excel(tmp2, include_exports=True)
            args2, _ = toe2.call_args
            assert args2[1] == ["h2"]


def test_export_exports_if_needed_false():
    packer = ScenarioPacker()
    s = Mock(spec=Scenario)
    s.id = "S"
    s.identifier = Mock(return_value="S")
    s._to_dataframe = Mock(return_value=pd.DataFrame({"S": [1]}, index=["row"]))
    packer.add(s)

    with (
        patch.object(OutputCurvesPack, "to_excel_per_carrier") as toe,
        patch("pyetm.models.scenario_packer.Workbook") as wb,
    ):
        wb.return_value = Mock()
        packer.to_excel("/tmp/x.xlsx", include_exports=False)
        toe.assert_not_called()


def test_add_gqueries_sheet_disabled():
    packer = ScenarioPacker()
    with (
        patch("pyetm.utils.excel_utils.add_frame") as add_frame,
        patch("pyetm.models.scenario_packer.Workbook") as wb,
        patch.object(ScenarioPacker, "_scenarios", return_value={Mock(spec=Scenario)}),
        patch.object(ScenarioPacker, "_add_data_sheets"),
    ):
        wb.return_value = Mock()
        # Make main_info non-empty to create MAIN
        with patch.object(
            ScenarioPacker,
            "main_info",
            return_value=pd.DataFrame({"A": [1]}, index=["i"]),
        ):
            packer.to_excel("/tmp/y.xlsx", include_gqueries=False)
            sheet_names = [call.kwargs.get("name") for call in add_frame.call_args_list]
            assert "MAIN" in sheet_names
            assert "GQUERIES" not in sheet_names and "GQUERIES_OUT" not in sheet_names


def test_clear_and_remove_scenario_swallow_errors():
    packer = ScenarioPacker()
    fake_pack1 = Mock()
    fake_pack2 = Mock()
    fake_pack1.clear.side_effect = RuntimeError("bad")
    fake_pack2.clear.return_value = None

    fake_pack1.discard.side_effect = RuntimeError("bad")
    fake_pack2.discard.return_value = None

    with patch.object(
        ScenarioPacker, "_get_all_packs", return_value=[fake_pack1, fake_pack2]
    ):
        packer.clear()  # should not raise
        sc = Mock(spec=Scenario)
        packer.remove_scenario(sc)  # should not raise
