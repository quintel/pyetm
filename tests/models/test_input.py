import pytest

from pyetm.models import Input

@pytest.fixture
def float_input_json():
    return {
        "investment_costs_co2_ccs":
            {
                "min":-100.0,
                "max":300.0,
                "default":0.0,
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


@pytest.mark.parametrize(
    'json_fixture',
    ['float_input_json', 'enum_input_json', 'bool_input_json', 'disabled_input_json']
)
def test_input_from_json(json_fixture, request):
    input = Input.from_json(request.getfixturevalue(json_fixture))

    # Assert valid input
    assert input
