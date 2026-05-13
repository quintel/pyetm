"""
Tests for ExportDataCollection model.
"""

import json
import pandas as pd
import pytest
from pyetm.models.export_data_collection import ExportDataCollection
from pyetm.models.export_config import ExportConfig


@pytest.fixture
def sample_main_info():
    """Sample main info DataFrame"""
    return pd.DataFrame(
        {
            "scenario_1": ["My Scenario", "nl", 2050, 2019],
            "scenario_2": ["Another Scenario", "de", 2045, 2020],
        },
        index=["title", "area_code", "end_year", "start_year"],
    )


@pytest.fixture
def sample_inputs():
    """Sample inputs DataFrame"""
    return pd.DataFrame(
        {
            "scenario_1": [1000, 2000],
            "scenario_2": [1500, 2500],
        },
        index=["wind_capacity", "solar_capacity"],
    )


@pytest.fixture
def sample_custom_curves():
    """Sample custom curves dict"""
    return {
        "solar_profile": {
            "scenario_1": pd.Series([0.1, 0.2, 0.3], name="scenario_1"),
            "scenario_2": pd.Series([0.15, 0.25, 0.35], name="scenario_2"),
        },
        "wind_profile": {
            "scenario_1": pd.Series([0.5, 0.6, 0.7], name="scenario_1"),
            "scenario_2": pd.Series([0.55, 0.65, 0.75], name="scenario_2"),
        },
    }


@pytest.fixture
def sample_hourly_curves():
    """Sample hourly output curves dict"""
    return {
        "electricity_production": {
            "scenario_1": pd.DataFrame({"hour": [0, 1, 2], "value": [100, 110, 120]}),
            "scenario_2": pd.DataFrame({"hour": [0, 1, 2], "value": [105, 115, 125]}),
        }
    }


@pytest.fixture
def sample_annual_exports():
    """Sample annual exports dict"""
    return {
        "energy_flow": {
            "scenario_1": pd.DataFrame(
                {"carrier": ["electricity", "gas"], "value": [100, 200]}
            ),
            "scenario_2": pd.DataFrame(
                {"carrier": ["electricity", "gas"], "value": [110, 210]}
            ),
        }
    }


@pytest.fixture
def minimal_export_data(sample_main_info):
    """Minimal ExportDataCollection with only required fields"""
    config = ExportConfig()
    return ExportDataCollection(main_info=sample_main_info, config=config)


@pytest.fixture
def full_export_data(
    sample_main_info,
    sample_inputs,
    sample_custom_curves,
    sample_hourly_curves,
    sample_annual_exports,
):
    """Full ExportDataCollection with all fields populated"""
    config = ExportConfig(
        include_inputs=True,
        include_custom_curves=True,
        output_carriers=["electricity"],
        include_annual_exports=["energy_flow"],
    )

    return ExportDataCollection(
        main_info=sample_main_info,
        inputs=sample_inputs,
        sortables=pd.DataFrame({"scenario_1": [1, 2], "scenario_2": [3, 4]}),
        custom_curves=sample_custom_curves,
        hourly_output_curves=sample_hourly_curves,
        annual_exports=sample_annual_exports,
        gquery_results=pd.DataFrame({"scenario_1": [100], "scenario_2": [200]}),
        users=pd.DataFrame({"scenario_1": ["user1"], "scenario_2": ["user2"]}),
        config=config,
    )


class TestExportDataCollectionCreation:
    """Tests for creating ExportDataCollection instances"""

    def test_create_minimal(self, minimal_export_data):
        """Test creating with only required fields"""
        assert isinstance(minimal_export_data.main_info, pd.DataFrame)
        assert isinstance(minimal_export_data.config, ExportConfig)
        assert minimal_export_data.inputs is None
        assert minimal_export_data.sortables is None
        assert minimal_export_data.custom_curves is None
        assert minimal_export_data.hourly_output_curves is None
        assert minimal_export_data.annual_exports is None
        assert minimal_export_data.gquery_results is None
        assert minimal_export_data.users is None

    def test_create_full(self, full_export_data):
        """Test creating with all fields populated"""
        assert isinstance(full_export_data.main_info, pd.DataFrame)
        assert isinstance(full_export_data.inputs, pd.DataFrame)
        assert isinstance(full_export_data.sortables, pd.DataFrame)
        assert isinstance(full_export_data.custom_curves, dict)
        assert isinstance(full_export_data.hourly_output_curves, dict)
        assert isinstance(full_export_data.annual_exports, dict)
        assert isinstance(full_export_data.gquery_results, pd.DataFrame)
        assert isinstance(full_export_data.users, pd.DataFrame)
        assert isinstance(full_export_data.config, ExportConfig)

    def test_main_info_required(self, sample_main_info):
        """Test that main_info is required"""
        config = ExportConfig()

        # Should work with main_info
        ExportDataCollection(main_info=sample_main_info, config=config)

        # Should fail without main_info
        with pytest.raises(Exception):  # Pydantic validation error
            ExportDataCollection(config=config)

    def test_config_required(self, sample_main_info):
        """Test that config is required"""
        # Should fail without config
        with pytest.raises(Exception):  # Pydantic validation error
            ExportDataCollection(main_info=sample_main_info)


class TestExportDataCollectionToDict:
    """Tests for to_dict() method"""

    def test_to_dict_minimal(self, minimal_export_data):
        """Test to_dict with minimal data"""
        result = minimal_export_data.to_dict()

        assert "main_info" in result
        assert "config" in result
        assert isinstance(result["main_info"], dict)
        assert isinstance(result["config"], dict)

        # Optional fields should not be in dict if None
        assert "inputs" not in result
        assert "custom_curves" not in result

    def test_to_dict_full(self, full_export_data):
        """Test to_dict with all data"""
        result = full_export_data.to_dict()

        # All fields should be present
        assert "main_info" in result
        assert "inputs" in result
        assert "sortables" in result
        assert "custom_curves" in result
        assert "hourly_output_curves" in result
        assert "annual_exports" in result
        assert "gquery_results" in result
        assert "users" in result
        assert "config" in result

        # Check nested structure
        assert isinstance(result["custom_curves"], dict)
        assert "solar_profile" in result["custom_curves"]
        assert "scenario_1" in result["custom_curves"]["solar_profile"]

    def test_to_dict_nested_structure(self, sample_custom_curves, sample_main_info):
        """Test that nested dicts are properly serialized"""
        config = ExportConfig()
        export_data = ExportDataCollection(
            main_info=sample_main_info,
            custom_curves=sample_custom_curves,
            config=config,
        )

        result = export_data.to_dict()

        # Check custom curves structure
        assert "solar_profile" in result["custom_curves"]
        assert "scenario_1" in result["custom_curves"]["solar_profile"]
        # Series should be converted to dict
        assert isinstance(result["custom_curves"]["solar_profile"]["scenario_1"], dict)

    def test_to_dict_dataframe_conversion(self, minimal_export_data):
        """Test that DataFrames are converted to dicts"""
        result = minimal_export_data.to_dict()

        # main_info should be a dict, not a DataFrame
        assert isinstance(result["main_info"], dict)
        assert not isinstance(result["main_info"], pd.DataFrame)


class TestExportDataCollectionRepr:
    """Tests for __repr__() method"""

    def test_repr_minimal(self, minimal_export_data):
        """Test repr with minimal data"""
        repr_str = repr(minimal_export_data)

        assert "ExportDataCollection" in repr_str
        assert "main_info:" in repr_str
        assert "scenarios" in repr_str

        # Optional fields should not appear
        assert "inputs:" not in repr_str
        assert "custom_curves:" not in repr_str

    def test_repr_full(self, full_export_data):
        """Test repr with all data"""
        repr_str = repr(full_export_data)

        assert "ExportDataCollection" in repr_str
        assert "main_info:" in repr_str
        assert "inputs:" in repr_str
        assert "sortables:" in repr_str
        assert "custom_curves:" in repr_str
        assert "hourly_output_curves:" in repr_str
        assert "annual_exports:" in repr_str
        assert "gquery_results:" in repr_str
        assert "users:" in repr_str

    def test_repr_shows_dimensions(self, minimal_export_data):
        """Test that repr shows data dimensions"""
        repr_str = repr(minimal_export_data)

        # Should show number of scenarios
        assert "2 scenarios" in repr_str or "scenario" in repr_str

    def test_repr_custom_curves_count(self, sample_custom_curves, sample_main_info):
        """Test that repr shows curve counts"""
        config = ExportConfig()
        export_data = ExportDataCollection(
            main_info=sample_main_info,
            custom_curves=sample_custom_curves,
            config=config,
        )

        repr_str = repr(export_data)

        # Should show number of curves
        assert "2 curves" in repr_str  # solar_profile and wind_profile


class TestExportDataCollectionImmutability:
    """Tests for data storage behavior"""

    def test_main_info_is_stored_as_reference(self, sample_main_info):
        """Test that DataFrames are stored as references (standard Pydantic behavior)"""
        config = ExportConfig()
        original_df = sample_main_info

        export_data = ExportDataCollection(main_info=original_df, config=config)

        # DataFrames are stored by reference (no deep copy)
        # This is expected behavior and matches Pydantic's handling of arbitrary types
        # Users should pass .copy() if they need independence
        assert export_data.main_info is original_df

    def test_users_can_pass_copies_for_independence(self, sample_main_info):
        """Test that users can pass copies if they need data independence"""
        config = ExportConfig()
        original_df = sample_main_info.copy()

        # User explicitly copies the DataFrame
        export_data = ExportDataCollection(main_info=original_df.copy(), config=config)

        # Now modify the original
        original_df.iloc[0, 0] = "Modified"

        # Export data is unchanged because user passed a copy
        assert export_data.main_info.iloc[0, 0] != "Modified"

    def test_config_is_stored_properly(self, sample_main_info):
        """Test that config is stored and accessible"""
        config = ExportConfig(include_inputs=True)
        export_data = ExportDataCollection(main_info=sample_main_info, config=config)

        # Config is accessible and has the expected value
        assert export_data.config.include_inputs is True

        # Note: Pydantic models are mutable by default, but this is expected behavior
        # Users should create a new ExportConfig if they need different settings


class TestMultiIndexConversion:
    """Tests for MultiIndex DataFrame to nested dict conversion"""

    @pytest.fixture
    def multiindex_main_info(self):
        """Sample main info DataFrame with MultiIndex columns"""
        columns = pd.MultiIndex.from_tuples(
            [
                ("scenario_5", "title"),
                ("scenario_5", "area_code"),
                ("scenario_5", "end_year"),
                ("scenario_7", "title"),
                ("scenario_7", "area_code"),
                ("scenario_7", "end_year"),
            ]
        )
        data = [["My Scenario", "nl", 2050, "Another", "de", 2045]]
        return pd.DataFrame(data, columns=columns, index=[0])

    @pytest.fixture
    def multiindex_inputs(self):
        """Sample inputs DataFrame with MultiIndex columns"""
        columns = pd.MultiIndex.from_tuples(
            [
                ("scenario_5", "wind_capacity"),
                ("scenario_5", "solar_capacity"),
                ("scenario_7", "wind_capacity"),
                ("scenario_7", "solar_capacity"),
            ]
        )
        data = [[1000, 2000, 1500, 2500]]
        return pd.DataFrame(data, columns=columns, index=[0])

    def test_safe_to_dict(self, multiindex_main_info):
        """Test that MultiIndex DataFrames are converted to nested dicts"""
        config = ExportConfig()
        export_data = ExportDataCollection(
            main_info=multiindex_main_info, config=config
        )

        result = export_data.to_dict()

        # Should be nested: {scenario_id: {field: value}}
        assert isinstance(result["main_info"], dict)
        assert "scenario_5" in result["main_info"]
        assert "scenario_7" in result["main_info"]

        # Each scenario should have its fields
        assert "title" in result["main_info"]["scenario_5"]
        assert "area_code" in result["main_info"]["scenario_5"]
        assert "end_year" in result["main_info"]["scenario_5"]

    def test_multiindex_to_dict_json_serializable(
        self, multiindex_main_info, multiindex_inputs
    ):
        """Test that MultiIndex DataFrames produce JSON-serializable output"""
        config = ExportConfig(include_inputs=True)
        export_data = ExportDataCollection(
            main_info=multiindex_main_info,
            inputs=multiindex_inputs,
            config=config,
        )

        result = export_data.to_dict()

        # Should be JSON serializable (no tuple keys)
        try:
            json_str = json.dumps(result, default=str)
            assert isinstance(json_str, str)
            # Verify we can parse it back
            parsed = json.loads(json_str)
            assert isinstance(parsed, dict)
        except TypeError as e:
            pytest.fail(f"Result is not JSON serializable: {e}")

    def test_multiindex_preserves_data(self, multiindex_main_info):
        """Test that conversion preserves all data values"""
        config = ExportConfig()
        export_data = ExportDataCollection(
            main_info=multiindex_main_info, config=config
        )

        result = export_data.to_dict()

        # Check that values are preserved
        assert result["main_info"]["scenario_5"]["title"][0] == "My Scenario"
        assert result["main_info"]["scenario_5"]["area_code"][0] == "nl"
        assert result["main_info"]["scenario_5"]["end_year"][0] == 2050
        assert result["main_info"]["scenario_7"]["title"][0] == "Another"

    def test_non_multiindex_still_works(self, sample_main_info):
        """Test that regular (non-MultiIndex) DataFrames still work"""
        config = ExportConfig()
        export_data = ExportDataCollection(main_info=sample_main_info, config=config)

        result = export_data.to_dict()

        # Should still return a dict
        assert isinstance(result["main_info"], dict)
        # For non-MultiIndex, should have column names as keys
        assert "scenario_1" in result["main_info"]
        assert "scenario_2" in result["main_info"]

    def test_multiindex_with_all_fields(self, multiindex_main_info, multiindex_inputs):
        """Test MultiIndex conversion with multiple DataFrame fields"""
        config = ExportConfig(include_inputs=True)

        # Create MultiIndex gquery_results
        gquery_columns = pd.MultiIndex.from_tuples(
            [("scenario_5", "query1"), ("scenario_7", "query1")]
        )
        gquery_results = pd.DataFrame([[100, 200]], columns=gquery_columns, index=[0])

        export_data = ExportDataCollection(
            main_info=multiindex_main_info,
            inputs=multiindex_inputs,
            gquery_results=gquery_results,
            config=config,
        )

        result = export_data.to_dict()

        # All MultiIndex fields should be nested dicts
        assert "scenario_5" in result["main_info"]
        assert "scenario_5" in result["inputs"]
        assert "scenario_5" in result["gquery_results"]

        # Should be JSON serializable
        json_str = json.dumps(result, default=str)
        assert isinstance(json_str, str)

    def test_multiindex_on_rows(self):
        """Test that DataFrames with MultiIndex rows are converted properly"""
        config = ExportConfig(include_annual_exports=["energy_flow"])

        # Create DataFrame with MultiIndex rows
        idx = pd.MultiIndex.from_tuples(
            [
                ("electricity", "import"),
                ("electricity", "export"),
                ("gas", "import"),
            ],
            names=["carrier", "direction"],
        )
        energy_flow_df = pd.DataFrame({"value": [100, 50, 200]}, index=idx)

        main_info = pd.DataFrame(
            {"scenario_5": ["Test", "nl", 2050]},
            index=["title", "area_code", "end_year"],
        )

        annual_exports = {"energy_flow": {"scenario_5": energy_flow_df}}

        export_data = ExportDataCollection(
            main_info=main_info, annual_exports=annual_exports, config=config
        )

        result = export_data.to_dict()

        # Should be JSON serializable (tuple keys converted to strings)
        json_str = json.dumps(result, default=str)
        assert isinstance(json_str, str)

        # Verify structure
        assert "annual_exports" in result
        assert "energy_flow" in result["annual_exports"]
        assert "scenario_5" in result["annual_exports"]["energy_flow"]

    def test_nested_dataframes_with_multiindex(self):
        """Test that nested DataFrames (hourly_output_curves, annual_exports) handle MultiIndex"""
        config = ExportConfig(
            output_carriers=["electricity"], include_annual_exports=["energy_flow"]
        )

        main_info = pd.DataFrame({"scenario_5": ["Test"]}, index=["title"])

        # Hourly output curves
        hourly_df = pd.DataFrame({"hour": [0, 1, 2], "value": [100, 110, 120]})
        hourly_output_curves = {"electricity_production": {"scenario_5": hourly_df}}

        # Annual exports with regular DataFrame
        annual_df = pd.DataFrame(
            {"carrier": ["electricity", "gas"], "value": [100, 200]}
        )
        annual_exports = {"energy_flow": {"scenario_5": annual_df}}

        export_data = ExportDataCollection(
            main_info=main_info,
            hourly_output_curves=hourly_output_curves,
            annual_exports=annual_exports,
            config=config,
        )

        result = export_data.to_dict()

        # Should be JSON serializable
        json_str = json.dumps(result, default=str)
        assert isinstance(json_str, str)

        # Verify nested structures exist
        assert "hourly_output_curves" in result
        assert "electricity_production" in result["hourly_output_curves"]
        assert "annual_exports" in result
        assert "energy_flow" in result["annual_exports"]

    def test_custom_curves_with_multiindex_series(self):
        """Test that custom curves with MultiIndex Series are handled"""
        config = ExportConfig(include_custom_curves=True)

        main_info = pd.DataFrame({"scenario_5": ["Test"]}, index=["title"])

        # Create Series with MultiIndex
        idx = pd.MultiIndex.from_tuples([(0, "a"), (0, "b"), (1, "a")])
        curve_series = pd.Series([0.1, 0.2, 0.3], index=idx)

        custom_curves = {"solar_profile": {"scenario_5": curve_series}}

        export_data = ExportDataCollection(
            main_info=main_info, custom_curves=custom_curves, config=config
        )

        result = export_data.to_dict()

        # Should be JSON serializable
        json_str = json.dumps(result, default=str)
        assert isinstance(json_str, str)

        # Verify structure
        assert "custom_curves" in result
        assert "solar_profile" in result["custom_curves"]


class TestExportDataCollectionEdgeCases:
    """Tests for edge cases and error conditions"""

    def test_empty_main_info(self):
        """Test with empty main_info DataFrame"""
        config = ExportConfig()
        empty_df = pd.DataFrame()

        export_data = ExportDataCollection(main_info=empty_df, config=config)

        assert export_data.main_info.empty
        assert isinstance(export_data.main_info, pd.DataFrame)

    def test_empty_nested_dicts(self, sample_main_info):
        """Test with empty nested dictionaries"""
        config = ExportConfig()

        export_data = ExportDataCollection(
            main_info=sample_main_info,
            custom_curves={},
            hourly_output_curves={},
            annual_exports={},
            config=config,
        )

        # Empty dicts should be preserved
        assert export_data.custom_curves == {}
        assert export_data.hourly_output_curves == {}
        assert export_data.annual_exports == {}

    def test_single_scenario(self):
        """Test with single scenario"""
        config = ExportConfig()
        single_scenario_df = pd.DataFrame(
            {"scenario_1": ["Test", "nl", 2050]},
            index=["title", "area_code", "end_year"],
        )

        export_data = ExportDataCollection(main_info=single_scenario_df, config=config)

        repr_str = repr(export_data)
        assert "1 scenario" in repr_str or "scenario" in repr_str

    def test_large_nested_structure(self, sample_main_info):
        """Test with many curves/exports"""
        config = ExportConfig()

        # Create many custom curves
        many_curves = {
            f"curve_{i}": {
                "scenario_1": pd.Series([1, 2, 3]),
                "scenario_2": pd.Series([4, 5, 6]),
            }
            for i in range(50)
        }

        export_data = ExportDataCollection(
            main_info=sample_main_info,
            custom_curves=many_curves,
            config=config,
        )

        assert len(export_data.custom_curves) == 50
        repr_str = repr(export_data)
        assert "50 curves" in repr_str
