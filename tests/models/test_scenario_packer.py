import pytest
import pandas as pd
import numpy as np
import tempfile
import os
from unittest.mock import Mock
from pyetm.models import ScenarioPacker, Scenario

class TestScenarioPackerInit:

    def test_init_creates_empty_collections(self):
        """Test that initialization creates empty collections"""
        packer = ScenarioPacker()

        assert isinstance(packer._scenarios, dict)
        assert len(packer._scenarios) == 4
        assert all(
            isinstance(collection, set) for collection in packer._scenarios.values()
        )
        assert all(len(collection) == 0 for collection in packer._scenarios.values())

    def test_init_with_pydantic_data(self):
        """Test initialization with Pydantic data"""
        packer = ScenarioPacker()
        assert packer._scenarios is not None


class TestScenarioPackerAdd:

    def test_add_single_scenario(self, sample_scenario):
        """Test adding a single scenario"""
        packer = ScenarioPacker()
        packer.add(sample_scenario)

        # Should be added to all collections
        for collection in packer._scenarios.values():
            assert sample_scenario in collection

    def test_add_multiple_scenarios(self, multiple_scenarios):
        """Test adding multiple scenarios at once"""
        packer = ScenarioPacker()
        packer.add(*multiple_scenarios)

        # All scenarios should be in all collections
        for collection in packer._scenarios.values():
            assert len(collection) == 3
            for scenario in multiple_scenarios:
                assert scenario in collection

    def test_add_custom_curves(self, sample_scenario):
        """Test adding scenarios to custom_curves only"""
        packer = ScenarioPacker()
        packer.add_custom_curves(sample_scenario)

        assert sample_scenario in packer._scenarios["custom_curves"]
        assert sample_scenario not in packer._scenarios["inputs"]
        assert sample_scenario not in packer._scenarios["sortables"]
        assert sample_scenario not in packer._scenarios["carrier_curves"]

    def test_add_inputs(self, sample_scenario):
        """Test adding scenarios to inputs only"""
        packer = ScenarioPacker()
        packer.add_inputs(sample_scenario)

        assert sample_scenario in packer._scenarios["inputs"]
        assert sample_scenario not in packer._scenarios["custom_curves"]

    def test_add_sortables(self, sample_scenario):
        """Test adding scenarios to sortables only"""
        packer = ScenarioPacker()
        packer.add_sortables(sample_scenario)

        assert sample_scenario in packer._scenarios["sortables"]
        assert sample_scenario not in packer._scenarios["inputs"]

    def test_add_carrier_curves(self, sample_scenario):
        """Test adding scenarios to carrier_curves only"""
        packer = ScenarioPacker()
        packer.add_carrier_curves(sample_scenario)

        assert sample_scenario in packer._scenarios["carrier_curves"]
        assert sample_scenario not in packer._scenarios["inputs"]


class TestScenarioPackerDataExtraction:

    def test_all_scenarios_empty(self):
        """Test _all_scenarios with empty packer"""
        packer = ScenarioPacker()
        assert len(packer._all_scenarios()) == 0

    def test_all_scenarios_single_collection(self, sample_scenario):
        """Test _all_scenarios with one collection"""
        packer = ScenarioPacker()
        packer.add_inputs(sample_scenario)

        all_scenarios = packer._all_scenarios()
        assert len(all_scenarios) == 1
        assert sample_scenario in all_scenarios

    def test_all_scenarios_multiple_collections(self, multiple_scenarios):
        """Test _all_scenarios with scenarios in different collections"""
        packer = ScenarioPacker()
        packer.add_inputs(multiple_scenarios[0])
        packer.add_custom_curves(multiple_scenarios[1])
        packer.add_sortables(multiple_scenarios[2])

        all_scenarios = packer._all_scenarios()
        assert len(all_scenarios) == 3
        for scenario in multiple_scenarios:
            assert scenario in all_scenarios

    def test_all_scenarios_overlapping_collections(self, sample_scenario):
        """Test _all_scenarios with same scenario in multiple collections"""
        packer = ScenarioPacker()
        packer.add_inputs(sample_scenario)
        packer.add_custom_curves(sample_scenario)

        all_scenarios = packer._all_scenarios()
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
        # Mock the to_dataframe method
        mock_df = pd.DataFrame(
            {"metadata": ["nl2015", 2050, "test_value"]},
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
                {"metadata": ["nl2015", 2050, f"value_{i}"]},
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
        # Mock the inputs.to_dataframe method
        mock_df = pd.DataFrame(
            {"value": [1000, 2000], "unit": ["MW", "MW"], "default": [500, 800]},
            index=["wind_capacity", "solar_capacity"],
        )

        scenario_with_inputs.inputs.to_dataframe = Mock(return_value=mock_df)

        packer = ScenarioPacker()
        packer.add_inputs(scenario_with_inputs)

        result = packer.inputs()

        assert not result.empty
        assert result.index.name == "input"
        assert ("unit", "") in result.columns
        assert (scenario_with_inputs.id, "value") in result.columns
        assert (scenario_with_inputs.id, "default") in result.columns

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
            scenario.inputs.to_dataframe = Mock(return_value=mock_df)

        packer = ScenarioPacker()
        packer.add_inputs(*multiple_scenarios)

        result = packer.inputs()

        # Should have unit column plus value/default for each scenario
        expected_columns = (
            [("unit", "")]
            + [(s.id, "value") for s in multiple_scenarios]
            + [(s.id, "default") for s in multiple_scenarios]
        )
        assert len(result.columns) >= len(multiple_scenarios) * 2 + 1

        # Should have all unique input keys
        all_keys = {
            "wind_capacity",
            "unique_input_0",
            "unique_input_1",
            "unique_input_2",
        }
        assert set(result.index) == all_keys


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

        packer = ScenarioPacker()
        packer.add(sample_scenario)

        result = packer.gquery_results()
        assert result.empty

    def test_gquery_results_single_scenario(self, scenario_with_queries):
        """Test gquery_results with single scenario"""
        packer = ScenarioPacker()
        packer.add(scenario_with_queries)

        result = packer.gquery_results()

        assert not result.empty
        assert result.index.name == "gquery"
        assert "unit" in result.columns
        assert scenario_with_queries.id in result.columns
        assert len(result) == 3

    def test_gquery_results_multiple_scenarios(self):
        """Test gquery_results with multiple scenarios"""
        scenarios = []
        for i in range(2):
            scenario = Mock(spec=Scenario)
            scenario.id = f"query_scenario_{i}"
            scenario.area_code = "nl2015"
            scenario.end_year = 2050

            mock_results = pd.DataFrame(
                {"future": [100 + i * 10, 200 + i * 20], "unit": ["MW", "GWh"]},
                index=[f"query_1", f"query_{i+2}"],
            )

            scenario.results = Mock(return_value=mock_results)
            scenario.queries_requested = Mock(return_value=True)
            scenarios.append(scenario)

        packer = ScenarioPacker()
        packer.add(*scenarios)

        result = packer.gquery_results()

        assert not result.empty
        assert "unit" in result.columns
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
        assert "curve1" in result.columns
        assert "curve2" in result.columns

    def test_carrier_curves_empty(self):
        """Test carrier_curves with no scenarios"""
        packer = ScenarioPacker()
        result = packer.carrier_curves()

        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_carrier_curves_with_series(self, sample_scenario):
        """Test carrier_curves with series data"""
        mock_series = pd.Series([10, 20, 30], name="carrier_curve")
        sample_scenario.carrier_curves_series = Mock(return_value=[mock_series])

        packer = ScenarioPacker()
        packer.add_carrier_curves(sample_scenario)
        result = packer.carrier_curves()
        assert not result.empty
        assert "carrier_curve" in result.columns


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
        # Mock all the data methods
        scenario_with_inputs.to_dataframe = Mock(
            return_value=pd.DataFrame(
                {"metadata": ["nl2015", 2050]}, index=["area_code", "end_year"]
            )
        )

        scenario_with_inputs.inputs.to_dataframe = Mock(
            return_value=pd.DataFrame(
                {"value": [1000], "unit": ["MW"]}, index=["wind_capacity"]
            )
        )

        scenario_with_inputs.queries_requested = Mock(return_value=False)

        packer = ScenarioPacker()
        packer.add(scenario_with_inputs)

        file_path = os.path.join(self.temp_dir, "test_export.xlsx")
        packer.to_excel(file_path)

        # Verify file was created
        assert os.path.exists(file_path)
        assert os.path.getsize(file_path) > 0

    def test_to_excel_all_sheet_types(self):
        """Test to_excel with all types of data"""
        scenario = Mock(spec=Scenario)
        scenario.id = "full_scenario"
        scenario.area_code = "nl2015"
        scenario.end_year = 2050

        # Mock all data methods to return non-empty DataFrames
        scenario.to_dataframe = Mock(
            return_value=pd.DataFrame({"col": [1]}, index=["row"])
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
        scenario.carrier_curves_series = Mock(
            return_value=[pd.Series([3, 4], name="carrier1")]
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
        assert len(packer._all_scenarios()) == 3

        packer.clear()

        # Verify all collections are empty
        assert len(packer._all_scenarios()) == 0
        for collection in packer._scenarios.values():
            assert len(collection) == 0

    def test_remove_scenario(self, multiple_scenarios):
        """Test remove_scenario method"""
        packer = ScenarioPacker()
        packer.add(*multiple_scenarios)

        scenario_to_remove = multiple_scenarios[1]
        packer.remove_scenario(scenario_to_remove)

        # Verify scenario is removed from all collections
        assert scenario_to_remove not in packer._all_scenarios()
        assert len(packer._all_scenarios()) == 2

        # Verify other scenarios remain
        for scenario in [multiple_scenarios[0], multiple_scenarios[2]]:
            assert scenario in packer._all_scenarios()

    def test_remove_scenario_not_present(self, sample_scenario):
        """Test removing scenario that's not in packer"""
        packer = ScenarioPacker()
        other_scenario = Scenario(id="other", area_code="de", end_year=2030)

        packer.add(sample_scenario)

        # Should not raise error
        packer.remove_scenario(other_scenario)

        # Original scenario should still be there
        assert sample_scenario in packer._all_scenarios()

    def test_get_summary_empty(self):
        """Test get_summary with empty packer"""
        packer = ScenarioPacker()
        summary = packer.get_summary()

        assert summary["total_scenarios"] == 0
        assert summary["custom_curves_count"] == 0
        assert summary["inputs_count"] == 0
        assert summary["sortables_count"] == 0
        assert summary["carrier_curves_count"] == 0
        assert summary["scenario_ids"] == []

    def test_get_summary_with_data(self, multiple_scenarios):
        """Test get_summary with scenarios"""
        packer = ScenarioPacker()
        packer.add_inputs(multiple_scenarios[0])
        packer.add_custom_curves(multiple_scenarios[1])
        packer.add(multiple_scenarios[2])

        summary = packer.get_summary()

        assert summary["total_scenarios"] == 3
        assert summary["inputs_count"] == 2  # scenarios 0 and 2
        assert summary["custom_curves_count"] == 2  # scenarios 1 and 2
        assert summary["sortables_count"] == 1  # scenario 2 only
        assert summary["carrier_curves_count"] == 1  # scenario 2 only
        assert len(summary["scenario_ids"]) == 3
        assert all(s.id in summary["scenario_ids"] for s in multiple_scenarios)


class TestPrivateHelperMethods:

    def test_get_all_input_keys(self):
        """Test _get_all_input_keys method"""
        packer = ScenarioPacker()

        df1 = pd.DataFrame(index=["key1", "key2"])
        df2 = pd.DataFrame(index=["key2", "key3"])
        scenario_dataframes = {"s1": df1, "s2": df2}

        result = packer._get_all_input_keys(scenario_dataframes)

        assert result == ["key1", "key2", "key3"]  # Sorted unique keys

    def test_get_value_column(self):
        """Test _get_value_column method"""
        packer = ScenarioPacker()

        # Test with 'value' column
        df1 = pd.DataFrame(columns=["value", "unit", "default"])
        assert packer._get_value_column(df1) == "value"

        # Test without 'value' column
        df2 = pd.DataFrame(columns=["future", "unit", "default"])
        assert packer._get_value_column(df2) == "future"

        # Test with only special columns
        df3 = pd.DataFrame(columns=["unit", "default"])
        assert packer._get_value_column(df3) is None

    def test_get_query_value_column(self):
        """Test _get_query_value_column method"""
        packer = ScenarioPacker()

        # Test with 'future' column
        df1 = pd.DataFrame(columns=["future", "unit"])
        assert packer._get_query_value_column(df1) == "future"

        # Test with 'present' column
        df2 = pd.DataFrame(columns=["present", "unit"])
        assert packer._get_query_value_column(df2) == "present"

        # Test with other column
        df3 = pd.DataFrame(columns=["other", "unit"])
        assert packer._get_query_value_column(df3) == "other"

        # Test with only unit column
        df4 = pd.DataFrame(columns=["unit"])
        assert packer._get_query_value_column(df4) is None

    def test_find_query_value(self):
        """Test _find_query_value method"""
        packer = ScenarioPacker()

        df = pd.DataFrame({"future": [100, 200]}, index=["query1", "query2"])

        # Test finding existing query
        result = packer._find_query_value(df, "query1", "future")
        assert result == 100

        # Test finding non-existing query
        result = packer._find_query_value(df, "query3", "future")
        assert result == ""

        # Test with None value_column
        result = packer._find_query_value(df, "query1", None)
        assert result == ""
