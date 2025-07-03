from pydantic import ValidationError
import pytest
from datetime import datetime
from pyetm.models.custom_curves import CustomCurves
from pyetm.models.scenario import Scenario, ScenarioError
from pyetm.services.scenario_runners.fetch_custom_curves import FetchAllCurveDataRunner
from pyetm.services.scenario_runners.fetch_inputs import FetchInputsRunner
from pyetm.services.scenario_runners.fetch_metadata import FetchMetadataRunner
from pyetm.services.scenario_runners.fetch_sortables import FetchSortablesRunner
from pyetm.services.service_result import ServiceResult
from pyetm.models.input_collection import InputCollection
from pyetm.models.sortable_collection import SortableCollection


# --- Fixtures --- #
# TODO: move fixtures out of individual tests into conftest or something


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


# --- Scenario.load tests --- #


def test_load_success(monkeypatch, full_scenario_metadata):
    """Test successful scenario load with complete metadata"""

    def fake_run(client, stub):
        return ServiceResult.ok(data=full_scenario_metadata)

    monkeypatch.setattr(FetchMetadataRunner, "run", fake_run)

    scenario = Scenario.load(1)
    for key, val in full_scenario_metadata.items():
        assert getattr(scenario, key) == val
    assert scenario.warnings == []


def test_load_with_warnings(monkeypatch, minimal_scenario_metadata):
    """Test scenario load with warnings from missing optional fields"""
    warns = ["Missing field in response: 'created_at'"]

    def fake_run(client, stub):
        return ServiceResult.ok(data=minimal_scenario_metadata, errors=warns)

    monkeypatch.setattr(FetchMetadataRunner, "run", fake_run)

    scenario = Scenario.load(2)
    assert scenario.id == 2
    assert scenario.end_year == 2040
    assert scenario.area_code == "NL"
    assert scenario.warnings == warns


def test_load_failure(monkeypatch):
    """Test scenario load failure"""

    def fake_run(client, stub):
        return ServiceResult.fail(["fatal error"])

    monkeypatch.setattr(FetchMetadataRunner, "run", fake_run)

    with pytest.raises(ScenarioError):
        Scenario.load(3)


def test_load_missing_required_field(monkeypatch):
    """Test scenario load fails when required fields are missing"""
    incomplete_data = {"id": 4}  # Missing end_year and area_code

    def fake_run(client, stub):
        return ServiceResult.ok(data=incomplete_data)

    monkeypatch.setattr(FetchMetadataRunner, "run", fake_run)

    # Should raise ScenarioError, but the underlying cause might be ValidationError
    with pytest.raises((ScenarioError, ValidationError, AttributeError)):
        Scenario.load(4)


# --- inputs property tests --- #


@pytest.fixture(autouse=True)
def patch_input_from_json(monkeypatch):
    dummy = object()
    monkeypatch.setattr(InputCollection, "from_json", staticmethod(lambda data: dummy))
    return dummy


def test_inputs_success(monkeypatch, patch_input_from_json, minimal_scenario_metadata):
    input_data = {"i1": {"min": 0.0}}

    def fake_run(client, scen):
        return ServiceResult.ok(data=input_data)

    monkeypatch.setattr(FetchInputsRunner, "run", fake_run)

    scenario = Scenario.model_validate(minimal_scenario_metadata)
    coll = scenario.inputs
    assert coll is patch_input_from_json
    assert scenario._inputs is coll
    assert scenario.warnings == []


def test_inputs_with_warnings(
    monkeypatch, patch_input_from_json, minimal_scenario_metadata
):
    input_data = {"i2": {"default": 42}}
    warns = ["parsed default with fallback"]

    def fake_run(client, scen):
        return ServiceResult.ok(data=input_data, errors=warns)

    monkeypatch.setattr(FetchInputsRunner, "run", fake_run)

    scenario = Scenario.model_validate(minimal_scenario_metadata)
    coll = scenario.inputs
    assert coll is patch_input_from_json
    assert scenario.warnings == warns


def test_inputs_failure(monkeypatch, minimal_scenario_metadata):
    def fake_run(client, scen):
        return ServiceResult.fail(["input fetch failed"])

    monkeypatch.setattr(FetchInputsRunner, "run", fake_run)

    scenario = Scenario.model_validate(minimal_scenario_metadata)
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


def test_sortables_success(
    monkeypatch, patch_sortables_from_json, minimal_scenario_metadata
):
    sort_data = {"forecast_storage": [1, 2]}

    def fake_run(client, scen):
        return ServiceResult.ok(data=sort_data)

    monkeypatch.setattr(FetchSortablesRunner, "run", fake_run)

    scenario = Scenario.model_validate(minimal_scenario_metadata)
    coll = scenario.sortables
    assert coll is patch_sortables_from_json
    assert scenario._sortables is coll
    assert scenario.warnings == []


def test_sortables_with_warnings(
    monkeypatch, patch_sortables_from_json, minimal_scenario_metadata
):
    sort_data = {"hs": [0]}
    warns = ["partial sortables fetched"]

    def fake_run(client, scen):
        return ServiceResult.ok(data=sort_data, errors=warns)

    monkeypatch.setattr(FetchSortablesRunner, "run", fake_run)

    scenario = Scenario.model_validate(minimal_scenario_metadata)
    coll = scenario.sortables
    assert coll is patch_sortables_from_json
    assert scenario.warnings == warns


def test_sortables_failure(monkeypatch, minimal_scenario_metadata):
    def fake_run(client, scen):
        return ServiceResult.fail(["sortable fetch failed"])

    monkeypatch.setattr(FetchSortablesRunner, "run", fake_run)

    scenario = Scenario.model_validate(minimal_scenario_metadata)
    with pytest.raises(ScenarioError):
        _ = scenario.sortables


@pytest.fixture(autouse=True)
def patch_custom_curves_from_json(monkeypatch):
    dummy = object()
    monkeypatch.setattr(CustomCurves, "from_json", staticmethod(lambda data: dummy))
    return dummy


def test_custom_curves_success(
    monkeypatch, patch_custom_curves_from_json, minimal_scenario_metadata
):
    curves_data = [
        {"attached": True, "key": "interconnector_2_export"},
        {"attached": True, "key": "solar_pv_profile_1"},
        {"attached": False, "key": "wind_profile_1"},
    ]

    def fake_run(client, scen):
        return ServiceResult.ok(data=curves_data)

    monkeypatch.setattr(FetchAllCurveDataRunner, "run", fake_run)

    scenario = Scenario.model_validate(minimal_scenario_metadata)
    coll = scenario.custom_curves
    assert coll is patch_custom_curves_from_json
    assert scenario._custom_curves is coll
    assert scenario.warnings == []


def test_custom_curves_with_warnings(
    monkeypatch, patch_custom_curves_from_json, minimal_scenario_metadata
):
    curves_data = [{"attached": True, "key": "incomplete_curve"}]
    warns = ["some curves could not be loaded"]

    def fake_run(client, scen):
        return ServiceResult.ok(data=curves_data, errors=warns)

    monkeypatch.setattr(FetchAllCurveDataRunner, "run", fake_run)

    scenario = Scenario.model_validate(minimal_scenario_metadata)
    coll = scenario.custom_curves
    assert coll is patch_custom_curves_from_json
    assert scenario.warnings == warns


def test_custom_curves_failure(monkeypatch, minimal_scenario_metadata):
    def fake_run(client, scen):
        return ServiceResult.fail(["custom curves fetch failed"])

    monkeypatch.setattr(FetchAllCurveDataRunner, "run", fake_run)

    scenario = Scenario.model_validate(minimal_scenario_metadata)
    with pytest.raises(ScenarioError):
        _ = scenario.custom_curves


# --- Validation tests --- #


def test_end_year_greater_than_start_year(minimal_scenario_metadata):
    """Test that end_year must be greater than start_year"""
    invalid_data = minimal_scenario_metadata.copy()
    invalid_data.update({"start_year": 2040, "end_year": 2030})

    with pytest.raises(ValueError, match="End year .* must be greater than start year"):
        Scenario.model_validate(invalid_data)


def test_to_dataframe(scenario):
    scenario = Scenario(id=scenario.id, area_code='nl2015', end_year=2050)
    dataframe = scenario.to_dataframe()

    assert dataframe[scenario.id]['end_year'] == 2050
