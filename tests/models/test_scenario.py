import pytest
from datetime import datetime
from pyetm.models import Scenario
from pydantic import ValidationError

@pytest.fixture
def minimal_scenario_json():
    return {"id": 42} #TODO: Expand as the scenario model grows

@pytest.fixture
def full_scenario_json():
    # Every field populated with valid values
    return {
        "id": 123,
        "created_at": "2025-06-01T12:34:56Z",
        "updated_at": "2025-06-02T01:23:45Z",
        "end_year": 2050,
        "keep_compatible": True,
        "private": True,
        "preset_scenario_id": 7,
        "area_code": "GB",
        "source": "user_upload",
        "user_values": "dXNlcl92YWxz",
        "balanced_values": "YmFsYW5jZWRfdmFscw==",
        "metadata": "bWV0YWRhdGE=",
        "active_couplings": "Y3VwbGluZ19kYXRh"
    }

@pytest.fixture
def missing_id_json():
    return {"created_at": "2025-06-01T12:00:00Z"}

@pytest.fixture
def invalid_type_json():
    return {"id": "this is a string"}

@pytest.mark.parametrize(
    "json_fixture, expected_id",
    [
        ("minimal_scenario_json", None),
        ("full_scenario_json", 123),
    ]
)
def test_scenario_parse_success(json_fixture, expected_id, request):
    raw = request.getfixturevalue(json_fixture)
    scenario = Scenario.model_validate(raw)
    assert isinstance(scenario.id, int)
    if expected_id is not None:
        assert scenario.id == expected_id
    # created_at only appears on full
    if "created_at" in raw:
        assert isinstance(scenario.created_at, datetime)
    else:
        assert scenario.created_at is None

@pytest.mark.parametrize(
    "json_fixture",
    ["missing_id_json", "invalid_type_json"]
)
def test_scenario_parse_failure(json_fixture, request):
    raw = request.getfixturevalue(json_fixture)
    with pytest.raises(ValidationError):
        Scenario.model_validate(raw)
