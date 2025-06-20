import pytest
from pyetm.models import Scenario, InputCollection, BalancedInputCollection
from pyetm.services.scenario_runners import FetchInputsRunner, FetchScenarioRunner
from pyetm.services.service_result import ServiceResult
from pyetm.models.scenario import ScenarioError
from pydantic import ValidationError


@pytest.fixture
def minimal_scenario_json():
    return {"id": 42}


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
        "active_couplings": "Y3VwbGluZ19kYXRh",
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
    ],
)
def test_scenario_parse_success(json_fixture, expected_id, request):
    raw = request.getfixturevalue(json_fixture)
    scenario = Scenario.model_validate(raw)
    assert isinstance(scenario.id, int)
    if expected_id is not None:
        assert scenario.id == expected_id


@pytest.mark.parametrize("json_fixture", ["missing_id_json", "invalid_type_json"])
def test_scenario_parse_failure(json_fixture, request):
    raw = request.getfixturevalue(json_fixture)
    with pytest.raises(ValidationError):
        Scenario.model_validate(raw)


def test_inputs(requests_mock, input_collection_json):
    url = f"https://example.com/api/scenarios/999/inputs"
    requests_mock.get(url, status_code=200, json=input_collection_json)

    scenario = Scenario(id=999)

    assert scenario.inputs
    assert len(scenario.inputs.keys()) == 4
    first_input = first_input = next(iter(scenario.inputs))
    assert first_input.key == "investment_costs_co2_ccs"

    assert scenario.user_values() == {"investment_costs_co2_ccs": 10.0}


def test_inputs_failing(requests_mock):
    url = f"https://example.com/api/scenarios/999/inputs"
    requests_mock.get(url, status_code=500)

    scenario = Scenario(id=999)

    with pytest.raises(ScenarioError):
        assert scenario.inputs


def test_inputs_setter_bypasses_runner(monkeypatch, input_collection_json):
    """
    If you explicitly set `scenario.inputs`, the runner should never be called.
    """
    dummy = InputCollection.from_json(input_collection_json)

    monkeypatch.setattr(
        FetchInputsRunner,
        "run",
        lambda *args, **kwargs: pytest.fail("Runner was called!"),
    )

    scenario = Scenario(id=999)
    scenario.inputs = dummy  # use the setter

    assert scenario._inputs is dummy
    # Getter returns what was set
    assert scenario.inputs is dummy


def test_inputs_getter_caches_result(monkeypatch, input_collection_json):
    """
    First access calls FetchInputsRunner.run once; subsequent accesses return the cached InputCollection.
    """
    calls = []

    def fake_run(client, scenario_obj):
        calls.append(True)
        return ServiceResult(success=True, data=input_collection_json, status_code=200)

    monkeypatch.setattr(FetchInputsRunner, "run", fake_run)

    scenario = Scenario(id=999)
    first = scenario.inputs
    second = scenario.inputs

    assert isinstance(first, InputCollection)
    assert first is second
    assert len(calls) == 1


@pytest.fixture
def balanced_values_json():
    return {
        "investment_costs": 10.0,
        "carbon_price": 5.5,
        "use_renewables": False,
    }


def test_balanced_inputs_success(monkeypatch, balanced_values_json):
    """
    On a 200, .balanced_inputs returns a BalancedInputCollection
    and balanced_values() returns the raw dict.
    """
    calls = []

    def fake_run(client, scenario):
        calls.append(True)
        return ServiceResult(
            success=True,
            data={"balanced_values": balanced_values_json},
            status_code=200,
        )

    monkeypatch.setattr(FetchScenarioRunner, "run", fake_run)

    scenario = Scenario(id=999)
    bic = scenario.balanced_inputs
    assert isinstance(bic, BalancedInputCollection)
    got = {bi.key: bi.value for bi in bic.inputs}
    assert got == balanced_values_json

    assert scenario.balanced_values() == balanced_values_json
    assert len(calls) == 1


def test_balanced_inputs_caches(monkeypatch, balanced_values_json):
    """
    Subsequent accesses to .balanced_inputs do not call the runner again.
    """
    calls = []

    def fake_run(client, scenario):
        calls.append(True)
        return ServiceResult(
            success=True,
            data={"balanced_values": balanced_values_json},
            status_code=200,
        )

    monkeypatch.setattr(FetchScenarioRunner, "run", fake_run)

    scenario = Scenario(id=999)
    first = scenario.balanced_inputs
    second = scenario.balanced_inputs

    assert first is second
    assert len(calls) == 1


def test_balanced_inputs_setter_bypasses_runner(monkeypatch, balanced_values_json):
    """
    If you manually assign .balanced_inputs, the runner should never be called.
    """
    dummy = BalancedInputCollection.from_json(balanced_values_json)
    monkeypatch.setattr(
        FetchScenarioRunner,
        "run",
        lambda *args, **kwargs: pytest.fail("Runner was called!"),
    )

    scenario = Scenario(id=999)
    scenario.balanced_inputs = dummy

    assert scenario._balanced_inputs is dummy
    assert scenario.balanced_inputs is dummy


def test_balanced_inputs_failure(monkeypatch):
    """
    If the runner returns success=False, .balanced_inputs should raise ScenarioError.
    """

    def fake_run(client, scenario):
        return ServiceResult(success=False, errors=["error_message"], status_code=500)

    monkeypatch.setattr(FetchScenarioRunner, "run", fake_run)

    scenario = Scenario(id=999)
    with pytest.raises(ScenarioError) as excinfo:
        _ = scenario.balanced_inputs

    assert "error_message" in str(excinfo.value)
