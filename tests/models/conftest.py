"""
Centralized fixtures for model tests. They will automatically be included.
"""

# TODO: Convert the 'literal' fixtures into factory methods to be used in the other tests for more flexibility


from unittest.mock import Mock
import pandas as pd
import pytest
from datetime import datetime
from pathlib import Path
from pyetm.models.sortables import Sortables
from pyetm.models.scenario import Scenario


# --- Scenario Fixtures --- #


@pytest.fixture
def full_scenario_metadata():
    """Complete scenario metadata for testing full loads"""
    return {
        "id": 1,
        "created_at": datetime(2025, 6, 1, 12, 0),
        "updated_at": datetime(2025, 6, 2, 12, 0),
        "end_year": 2030,
        "keep_compatible": False,
        "private": True,
        "area_code": "NL",
        "source": "api",
        "metadata": {"foo": "bar"},
        "start_year": 2020,
        "scaling": None,
        "template": 5,
        "url": "http://example.com",
    }


@pytest.fixture
def minimal_scenario_metadata():
    """Minimal valid scenario metadata with only required fields"""
    return {"id": 2, "end_year": 2040, "area_code": "NL"}


@pytest.fixture
def scenario(minimal_scenario_metadata):
    """A basic Scenario instance for testing"""
    return Scenario.model_validate(minimal_scenario_metadata)


@pytest.fixture
def sample_scenario():
    """Create a sample scenario for testing"""
    # Use Mock to avoid Pydantic validation issues
    scenario = Mock(spec=Scenario)
    scenario.id = "test_scenario"
    scenario.area_code = "nl2015"
    scenario.end_year = 2050

    # Default mock methods that return empty lists/DataFrames
    scenario.custom_curves_series = Mock(return_value=[])
    scenario.carrier_curves_series = Mock(return_value=[])
    scenario.queries_requested = Mock(return_value=False)
    scenario.results = Mock(return_value=pd.DataFrame())

    # Mock sortables
    scenario.sortables = Mock()
    scenario.sortables.to_dataframe = Mock(return_value=pd.DataFrame())

    return scenario


@pytest.fixture
def scenario_with_inputs():
    """Create a scenario with input data"""
    scenario = Mock(spec=Scenario)
    scenario.id = "input_scenario"
    scenario.area_code = "nl2015"
    scenario.end_year = 2050

    # Mock the inputs collection
    scenario.inputs = Mock()

    # Set up default mock methods
    scenario.custom_curves_series = Mock(return_value=[])
    scenario.carrier_curves_series = Mock(return_value=[])
    scenario.queries_requested = Mock(return_value=False)
    scenario.results = Mock(return_value=pd.DataFrame())

    # Mock sortables
    scenario.sortables = Mock()
    scenario.sortables.to_dataframe = Mock(return_value=pd.DataFrame())

    return scenario


@pytest.fixture
def scenario_with_queries():
    """Create a scenario with query results"""
    scenario = Mock(spec=Scenario)
    scenario.id = "query_scenario"
    scenario.area_code = "nl2015"
    scenario.end_year = 2050

    # Mock the results method
    mock_results = pd.DataFrame(
        {
            "future": [100, 200, 300],
            "present": [90, 180, 270],
            "unit": ["MW", "GWh", "MT"],
        },
        index=["total_costs", "co2_emissions", "energy_demand"],
    )
    mock_results.index.name = "gquery"

    scenario.results = Mock(return_value=mock_results.set_index("unit", append=True))
    scenario.queries_requested = Mock(return_value=True)

    # Set up default mock methods
    scenario.custom_curves_series = Mock(return_value=[])
    scenario.carrier_curves_series = Mock(return_value=[])

    # Mock sortables
    scenario.sortables = Mock()
    scenario.sortables.to_dataframe = Mock(return_value=pd.DataFrame())

    return scenario


@pytest.fixture
def multiple_scenarios():
    """Create multiple scenarios for testing"""
    scenarios = []
    for i in range(3):
        scenario = Mock(spec=Scenario)
        scenario.id = f"scenario_{i}"
        scenario.area_code = "nl2015"
        scenario.end_year = 2050

        # Mock the inputs collection
        scenario.inputs = Mock()

        # Set up default mock methods
        scenario.custom_curves_series = Mock(return_value=[])
        scenario.carrier_curves_series = Mock(return_value=[])
        scenario.queries_requested = Mock(return_value=False)
        scenario.results = Mock(return_value=pd.DataFrame())

        # Mock sortables
        scenario.sortables = Mock()
        scenario.sortables.to_dataframe = Mock(return_value=pd.DataFrame())

        scenarios.append(scenario)
    return scenarios


@pytest.fixture(autouse=True)
def patch_sortables_from_json(monkeypatch):
    dummy = object()
    monkeypatch.setattr(Sortables, "from_json", staticmethod(lambda data: dummy))
    return dummy


# --- Input Fixtures --- #


@pytest.fixture
def float_input_json():
    """JSON data for a float input"""
    return {
        "investment_costs_co2_ccs": {
            "min": 0.0,
            "max": 1000.0,
            "default": 500.0,
            "step": 0.1,
            "unit": "EUR/tonne",
            "group": "costs",
        }
    }


@pytest.fixture
def enum_input_json():
    """JSON data for an enum input"""
    return {
        "transport_car_fuel_type": {
            "default": "gasoline",
            "permitted_values": ["gasoline", "diesel", "electric", "hydrogen"],
            "unit": "enum",
        }
    }


@pytest.fixture
def bool_input_json():
    """JSON data for a boolean input"""
    return {
        "has_electricity_storage": {"min": 0, "max": 1, "default": 0, "unit": "bool"}
    }


@pytest.fixture
def disabled_input_json():
    """JSON data for a disabled input"""
    return {
        "legacy_input": {
            "min": 0.0,
            "max": 100.0,
            "unit": "euros",
            "default": 50.0,
            "disabled": True,
        }
    }


@pytest.fixture
def inputs_json(
    float_input_json, enum_input_json, bool_input_json, disabled_input_json
):
    """Combined input collection JSON"""
    result = {}
    result.update(float_input_json)
    result.update(enum_input_json)
    result.update(bool_input_json)
    result.update(disabled_input_json)
    return result


# --- GQuery Fixtures --- #


@pytest.fixture
def valid_queries():
    return ["system_costs", "hydrogen_demand"]


# --- Sortable Fixtures --- #


@pytest.fixture
def sortable_collection_json():
    """
    Simulate the JSON returned by the index endpoint, with:
      - two flat lists
      - one nested heat_network dict
    """
    return {
        "forecast_storage": ["fs1", "fs2"],
        "heat_network": {"lt": ["hn_lt_1"], "mt": ["hn_mt_1", "hn_mt_2"], "ht": []},
        "hydrogen_supply": ["hs1"],
    }


@pytest.fixture
def valid_sortable_collection_json():
    """Fixture with valid data that won't trigger validation warnings"""
    return {
        "forecast_storage": ["fs1", "fs2"],
        "heat_network": {
            "lt": ["hn1", "hn2"],
            "mt": ["hn3"],
            "ht": ["hn4", "hn5", "hn6"],
        },
        "hydrogen_supply": ["hs1", "hs2", "hs3"],
    }


# --- Curve Fixtures --- #


@pytest.fixture
def custom_curves_json():
    """JSON data for custom curves"""
    return [
        {
            "attached": True,
            "key": "interconnector_2_export_availability",
            "type": "availability",
        },
        {"attached": True, "key": "solar_pv_profile_1", "type": "profile"},
        {"attached": False, "key": "wind_profile_1", "type": "profile"},
    ]


# --- Service Result Fixtures --- #


@pytest.fixture
def ok_service_result():
    """Factory fixture for creating successful ServiceResult objects"""
    from pyetm.services.service_result import ServiceResult

    def _make_result(data, errors=None):
        return ServiceResult.ok(data=data, errors=errors or [])

    return _make_result


@pytest.fixture
def fail_service_result():
    """Factory fixture for creating failed ServiceResult objects"""
    from pyetm.services.service_result import ServiceResult

    def _make_result(errors):
        return ServiceResult.fail(errors)

    return _make_result


# --- Test Model Fixtures --- #


@pytest.fixture
def dummy_base_model():
    """A dummy model class for testing Base functionality"""
    from pyetm.models.base import Base

    class Dummy(Base):
        a: int
        b: str
        c: float = 1.23  # default value

    return Dummy


# --- Path Fixtures --- #


@pytest.fixture
def fixture_path():
    """Path to the fixtures directory"""
    return Path(__file__).parent / "fixtures"


@pytest.fixture
def interconnector_csv_path(fixture_path):
    """Path to the interconnector CSV fixture file"""
    return fixture_path / "interconnector_2_export_availability.csv"
