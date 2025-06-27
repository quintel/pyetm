import pytest
from datetime import datetime
from pyetm.models import Scenario, InputCollection
from pyetm.models.sortable_collection import SortableCollection
from pyetm.services.scenario_runners import FetchInputsRunner
from pyetm.services.scenario_runners.fetch_sortables import FetchSortablesRunner
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
def sortable_collection_json():
    """
    Simulate the JSON returned by:
      GET /api/v3/scenarios/{id}/user_sortables
    """
    return {
        "forecast_storage": ["fs1", "fs2"],
        "heat_network": {"lt": ["a"], "mt": ["b", "c"], "ht": []},
        "hydrogen_supply": ["hs1"],
    }


def test_scenario_sortables_success(
    requests_mock, api_url, client, scenario, sortable_collection_json
):
    """
    200 → success=True, scenario.sortables returns a SortableCollection
            with one item per flat list and one per heat_network subtype.
    """
    url = f"{api_url}/scenarios/{scenario.id}/user_sortables"
    requests_mock.get(url, status_code=200, json=sortable_collection_json)

    coll = scenario.sortables
    assert isinstance(coll, SortableCollection)

    # 1 forecast_storage + 3 heat_network subtypes + 1 hydrogen_supply = 5
    assert len(coll) == 5

    assert coll.keys() == [
        "forecast_storage",
        "heat_network",
        "heat_network",
        "heat_network",
        "hydrogen_supply",
    ]

    first = next(iter(coll))
    assert first.type == "forecast_storage"
    assert first.subtype is None
    assert first.order == ["fs1", "fs2"]


def test_scenario_sortables_failure(requests_mock, api_url, client, scenario):
    """
    500 → success=False in runner → scenario.sortables raises ScenarioError.
    """
    url = f"{api_url}/scenarios/{scenario.id}/user_sortables"
    requests_mock.get(url, status_code=500)

    with pytest.raises(ScenarioError):
        _ = scenario.sortables


def test_scenario_sortables_setter_bypasses_runner(
    monkeypatch, client, scenario, sortable_collection_json
):
    """
    If you explicitly set `scenario.sortables`, the runner should never be called.
    """
    dummy = SortableCollection.from_json(sortable_collection_json)

    monkeypatch.setattr(
        FetchSortablesRunner,
        "run",
        lambda *args, **kwargs: pytest.fail("FetchSortablesRunner.run was called!"),
    )

    scenario.sortables = dummy  # use the setter

    assert scenario._sortables is dummy
    # Getter returns what was set
    assert scenario.sortables is dummy


def test_scenario_sortables_getter_caches_result(
    monkeypatch, client, scenario, sortable_collection_json
):
    """
    First access calls the runner exactly once; subsequent accesses return the cached SortableCollection.
    """
    calls = []

    def fake_run(cli, scen):
        calls.append(True)
        return ServiceResult(
            success=True, data=sortable_collection_json, status_code=200
        )

    monkeypatch.setattr(FetchSortablesRunner, "run", fake_run)

    first = scenario.sortables
    second = scenario.sortables

    assert isinstance(first, SortableCollection)
    assert first is second
    assert len(calls) == 1
