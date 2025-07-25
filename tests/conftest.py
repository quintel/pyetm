"""
Runs during test collection. You can also supply fixtures here that should be loaded
before each test
"""

from pydantic import HttpUrl
import os, sys, pytest

# Ensure src/ is on sys.path before any imports of your app code
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC = os.path.join(ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

# Set the ENV vars at import time so BaseClient picks up the test URL and token
os.environ["BASE_URL"] = "https://example.com/api"
os.environ["ETM_API_TOKEN"] = "etm_real.looking.token"


# Fixture to give back that same base URL for building expected mock URLs
@pytest.fixture
def api_url():
    return HttpUrl(os.getenv("BASE_URL"))


# Mount the requests-mock adapter onto BaseClient.session so that
# requests_mock.get(...) actually intercepts client.session.get(...)
@pytest.fixture(autouse=True)
def _mount_requests_mock(requests_mock, client):
    """
    requests_mock._adapter is the HTTPAdapter instance used
    by the pytest-requests-mock plugin.
    """
    adapter = getattr(requests_mock, "_adapter", None)
    if adapter and hasattr(client, "session") and hasattr(client.session, "session"):
        client.session.session.mount("http://", adapter)
        client.session.session.mount("https://", adapter)


# Lazy‐import BaseClient
@pytest.fixture
def client():
    from pyetm.clients.base_client import BaseClient

    return BaseClient()


# Lazy‐import Scenario
@pytest.fixture
def scenario():
    from pyetm.models import Scenario

    return Scenario(id=999)


@pytest.fixture
def float_input_json():
    return {
        "investment_costs_co2_ccs": {
            "min": -100.0,
            "max": 300.0,
            "default": 0.0,
            "user": 10.0,
            "unit": "%",
            "disabled": False,
        }
    }


@pytest.fixture
def enum_input_json():
    return {
        "settings_enable_storage_optimisation_households_flexibility_p2p_electricity": {
            "default": "default",
            "unit": "enum",
            "disabled": False,
            "permitted_values": [
                "default",
                "optimizing_storage",
                "optimizing_storage_households",
            ],
        }
    }


@pytest.fixture
def bool_input_json():
    return {
        "settings_enable_storage_optimisation_transport_car_flexibility_p2p_electricity": {
            "min": 0.0,
            "max": 1.0,
            "default": 0.0,
            "unit": "bool",
            "disabled": False,
        }
    }


@pytest.fixture
def disabled_input_json():
    return {
        "external_coupling_energy_production_synthetic_kerosene_demand": {
            "min": 0.0,
            "max": 10000.0,
            "default": 0.0,
            "unit": "PJ",
            "disabled": True,
            "coupling_disabled": True,
            "coupling_groups": ["external_model_industry", "ccus"],
        }
    }


@pytest.fixture
def input_collection_json(
    float_input_json, enum_input_json, bool_input_json, disabled_input_json
):
    return float_input_json | enum_input_json | bool_input_json | disabled_input_json


@pytest.fixture
def custom_curves_json():
    return [
        {
            "key": "interconnector_2_export_availability",
            "type": "availability",
            "display_group": "interconnectors",
            "attached": True,
            "overrides": ["electricity_interconnector_2_export_availability"],
            "name": "interconnector_2_export_availability_curve",
            "size": 78843,
            "date": "2025-05-01T10:31:59.860Z",
            "stats": {"length": 8760, "min_at": 3, "max_at": 0},
        },
        {
            "key": "buildings_appliances",
            "type": "profile",
            "display_group": "buildings",
            "attached": True,
            "overrides": [],
            "name": "buildings_appliances_curve",
            "size": 78843,
            "date": "2025-05-01T10:31:59.185Z",
            "stats": {"length": 8760, "min_at": 4557, "max_at": 2194},
        },
        {
            "key": "weather/solar_pv_profile_1",
            "type": "capacity_profile",
            "display_group": "electricity_production",
            "attached": True,
            "overrides": [
                "flh_of_solar_pv_solar_radiation",
                "flh_of_solar_pv_solar_radiation_user_curve",
            ],
            "name": "weather/solar_pv_profile_1_curve",
            "size": 78843,
            "date": "2025-05-01T10:32:02.014Z",
            "stats": {
                "length": 8760,
                "min_at": 0,
                "max_at": 3323,
                "full_load_hours": 1000.0,
            },
        },
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
