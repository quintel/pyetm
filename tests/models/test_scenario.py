import pytest
from datetime import datetime
from pyetm.models.scenario import Scenario, ScenarioError
from pyetm.services.scenario_runners.fetch_inputs import FetchInputsRunner
from pyetm.services.scenario_runners.fetch_metadata import FetchMetadataRunner
from pyetm.services.scenario_runners.fetch_sortables import FetchSortablesRunner
from pyetm.services.service_result import ServiceResult
from pyetm.models.input_collection import InputCollection
from pyetm.models.sortable_collection import SortableCollection

# --- Scenario.load tests --- #


# TODO: Again fixturise this one
def test_load_success(monkeypatch):
    # simulate full metadata fetch with no warnings
    meta = {
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

    # Patch the class method directly
    def fake_run(client, stub):
        return ServiceResult.ok(data=meta)

    monkeypatch.setattr(FetchMetadataRunner, "run", fake_run)

    scenario = Scenario.load(1)
    for key, val in meta.items():
        assert getattr(scenario, key) == val
    assert scenario.warnings == []


def test_load_with_warnings(monkeypatch):
    # simulate fetch returning warnings
    minimal_meta = {"id": 2}
    warns = ["Missing field in response: 'created_at'"]

    def fake_run(client, stub):
        return ServiceResult.ok(data=minimal_meta, errors=warns)

    monkeypatch.setattr(FetchMetadataRunner, "run", fake_run)

    scenario = Scenario.load(2)
    assert scenario.id == 2
    assert scenario.warnings == warns


def test_load_failure(monkeypatch):
    def fake_run(client, stub):
        return ServiceResult.fail(["fatal error"])

    monkeypatch.setattr(FetchMetadataRunner, "run", fake_run)

    with pytest.raises(ScenarioError):
        Scenario.load(3)


# --- inputs property tests --- #


@pytest.fixture(autouse=True)
def patch_input_from_json(monkeypatch):
    dummy = object()
    monkeypatch.setattr(InputCollection, "from_json", staticmethod(lambda data: dummy))
    return dummy


def test_inputs_success(monkeypatch, patch_input_from_json):
    input_data = {"i1": {"min": 0.0}}

    def fake_run(client, scen):
        return ServiceResult.ok(data=input_data)

    monkeypatch.setattr(FetchInputsRunner, "run", fake_run)

    scenario = Scenario(id=5)
    coll = scenario.inputs
    assert coll is patch_input_from_json
    assert scenario._inputs is coll
    assert scenario.warnings == []


def test_inputs_with_warnings(monkeypatch, patch_input_from_json):
    input_data = {"i2": {"default": 42}}
    warns = ["parsed default with fallback"]

    def fake_run(client, scen):
        return ServiceResult.ok(data=input_data, errors=warns)

    monkeypatch.setattr(FetchInputsRunner, "run", fake_run)

    scenario = Scenario(id=6)
    coll = scenario.inputs
    assert coll is patch_input_from_json
    assert scenario.warnings == warns


def test_inputs_failure(monkeypatch):
    def fake_run(client, scen):
        return ServiceResult.fail(["input fetch failed"])

    monkeypatch.setattr(FetchInputsRunner, "run", fake_run)

    scenario = Scenario(id=7)
    with pytest.raises(ScenarioError):
        _ = scenario.inputs


# --- sortables property tests --- #


@pytest.fixture(autouse=True)
def patch_sortables_from_json(monkeypatch):
    dummy = object()
    monkeypatch.setattr(
        SortableCollection, "from_json", staticmethod(lambda data: dummy)
    )
    return dummy


def test_sortables_success(monkeypatch, patch_sortables_from_json):
    sort_data = {"forecast_storage": [1, 2]}

    def fake_run(client, scen):
        return ServiceResult.ok(data=sort_data)

    monkeypatch.setattr(FetchSortablesRunner, "run", fake_run)

    scenario = Scenario(id=8)
    coll = scenario.sortables
    assert coll is patch_sortables_from_json
    assert scenario._sortables is coll
    assert scenario.warnings == []


def test_sortables_with_warnings(monkeypatch, patch_sortables_from_json):
    sort_data = {"hs": [0]}
    warns = ["partial sortables fetched"]

    def fake_run(client, scen):
        return ServiceResult.ok(data=sort_data, errors=warns)

    monkeypatch.setattr(FetchSortablesRunner, "run", fake_run)

    scenario = Scenario(id=9)
    coll = scenario.sortables
    assert coll is patch_sortables_from_json
    assert scenario.warnings == warns


def test_sortables_failure(monkeypatch):
    def fake_run(client, scen):
        return ServiceResult.fail(["sortable fetch failed"])

    monkeypatch.setattr(FetchSortablesRunner, "run", fake_run)

    scenario = Scenario(id=10)
    with pytest.raises(ScenarioError):
        _ = scenario.sortables


def test_custom_curves(requests_mock, api_url, scenario, custom_curves_json):
    url = f"{api_url}/scenarios/{scenario.id}/custom_curves"
    requests_mock.get(url, status_code=200, json=custom_curves_json)

    assert scenario.custom_curves
    assert len(scenario.custom_curves) == 3
