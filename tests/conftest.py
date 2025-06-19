'''
Runs during test collection. You can also supply fixtures here that should be loaded
before each test
'''

import os, sys, importlib, pytest

os.environ['ETM_API_TOKEN']     = 'real-token'
os.environ['BASE_URL']          = 'https://example.com/api'

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC  = os.path.join(ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)


# Fixtures for inputs and input collections from JSON
@pytest.fixture
def float_input_json():
    return {
        "investment_costs_co2_ccs":
            {
                "min":-100.0,
                "max":300.0,
                "default":0.0,
                "user": 10.0,
                "unit":"%",
                "disabled": False
            }
    }

@pytest.fixture
def enum_input_json():
    return {
        "settings_enable_storage_optimisation_households_flexibility_p2p_electricity":
            {
                "default":"default",
                "unit":"enum",
                "disabled": False,
                "permitted_values":[
                    "default","optimizing_storage","optimizing_storage_households"
                ]
            }
    }

@pytest.fixture
def bool_input_json():
    return {
        "settings_enable_storage_optimisation_transport_car_flexibility_p2p_electricity":
            {
                "min":0.0,
                "max":1.0,
                "default":0.0,
                "unit":"bool",
                "disabled": False
            }
    }

@pytest.fixture
def disabled_input_json():
    return {
        "external_coupling_energy_production_synthetic_kerosene_demand":
            {
                "min":0.0,
                "max":10000.0,
                "default":0.0,
                "unit":"PJ",
                "disabled": True,
                "coupling_disabled": True,
                "coupling_groups":["external_model_industry","ccus"]
            }
    }

@pytest.fixture
def input_collection_json(float_input_json, enum_input_json, bool_input_json, disabled_input_json):
    return float_input_json | enum_input_json | bool_input_json | disabled_input_json
