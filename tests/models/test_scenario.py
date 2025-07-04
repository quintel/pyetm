from pydantic import ValidationError
import pytest
from pyetm.models.custom_curves import CustomCurves
from pyetm.models.scenario import Scenario, ScenarioError
from pyetm.services.scenario_runners.fetch_custom_curves import (
    FetchAllCustomCurveDataRunner,
)
from pyetm.services.scenario_runners.fetch_inputs import FetchInputsRunner
from pyetm.services.scenario_runners.fetch_metadata import FetchMetadataRunner
from pyetm.services.scenario_runners.fetch_sortables import FetchSortablesRunner
from pyetm.models.input_collection import InputCollection
from pyetm.models.sortable_collection import SortableCollection


# ------ Load ------ #


def test_load_success(monkeypatch, full_scenario_metadata, ok_service_result):
    """Test successful scenario load with complete metadata"""
    monkeypatch.setattr(
        FetchMetadataRunner,
        "run",
        lambda client, stub: ok_service_result(full_scenario_metadata),
    )

    scenario = Scenario.load(1)
    for key, val in full_scenario_metadata.items():
        assert getattr(scenario, key) == val
    assert scenario.warnings == []


def test_load_with_warnings(monkeypatch, minimal_scenario_metadata, ok_service_result):
    """Test scenario load with warnings from missing optional fields"""
    warns = ["Missing field in response: 'created_at'"]

    monkeypatch.setattr(
        FetchMetadataRunner,
        "run",
        lambda client, stub: ok_service_result(minimal_scenario_metadata, warns),
    )

    scenario = Scenario.load(2)
    assert scenario.id == 2
    assert scenario.end_year == 2040
    assert scenario.area_code == "NL"
    assert scenario.warnings == warns


def test_load_failure(monkeypatch, fail_service_result):
    """Test scenario load failure"""
    monkeypatch.setattr(
        FetchMetadataRunner,
        "run",
        lambda client, stub: fail_service_result(["fatal error"]),
    )

    with pytest.raises(ScenarioError):
        Scenario.load(3)


def test_load_missing_required_field(monkeypatch, ok_service_result):
    """Test scenario load fails when required fields are missing"""
    incomplete_data = {"id": 4}  # Missing end_year and area_code

    monkeypatch.setattr(
        FetchMetadataRunner,
        "run",
        lambda client, stub: ok_service_result(incomplete_data),
    )

    # Should raise ScenarioError, but the underlying cause might be ValidationError
    with pytest.raises((ScenarioError, ValidationError, AttributeError)):
        Scenario.load(4)


# ------- inputs ------- #


@pytest.fixture(autouse=True)
def patch_input_from_json(monkeypatch):
    dummy = object()
    monkeypatch.setattr(InputCollection, "from_json", staticmethod(lambda data: dummy))
    return dummy


def test_inputs_success(
    monkeypatch, patch_input_from_json, scenario, ok_service_result
):
    input_data = {"i1": {"min": 0.0}}

    monkeypatch.setattr(
        FetchInputsRunner, "run", lambda client, scen: ok_service_result(input_data)
    )

    coll = scenario.inputs
    assert coll is patch_input_from_json
    assert scenario._inputs is coll
    assert scenario.warnings == []


def test_inputs_with_warnings(
    monkeypatch, patch_input_from_json, scenario, ok_service_result
):
    input_data = {"i2": {"default": 42}}
    warns = ["parsed default with fallback"]

    monkeypatch.setattr(
        FetchInputsRunner,
        "run",
        lambda client, scen: ok_service_result(input_data, warns),
    )

    coll = scenario.inputs
    assert coll is patch_input_from_json
    assert scenario.warnings == warns


def test_inputs_failure(monkeypatch, scenario, fail_service_result):
    monkeypatch.setattr(
        FetchInputsRunner,
        "run",
        lambda client, scen: fail_service_result(["input fetch failed"]),
    )

    with pytest.raises(ScenarioError):
        _ = scenario.inputs


# ------ sortables ------ #


@pytest.fixture(autouse=True)
def patch_sortables_from_json(monkeypatch):
    dummy = object()
    monkeypatch.setattr(
        SortableCollection, "from_json", staticmethod(lambda data: dummy)
    )
    return dummy


def test_sortables_success(
    monkeypatch, patch_sortables_from_json, scenario, ok_service_result
):
    sort_data = {"forecast_storage": [1, 2]}

    monkeypatch.setattr(
        FetchSortablesRunner, "run", lambda client, scen: ok_service_result(sort_data)
    )

    coll = scenario.sortables
    assert coll is patch_sortables_from_json
    assert scenario._sortables is coll
    assert scenario.warnings == []


def test_sortables_with_warnings(
    monkeypatch, patch_sortables_from_json, scenario, ok_service_result
):
    sort_data = {"hs": [0]}
    warns = ["partial sortables fetched"]

    monkeypatch.setattr(
        FetchSortablesRunner,
        "run",
        lambda client, scen: ok_service_result(sort_data, warns),
    )

    coll = scenario.sortables
    assert coll is patch_sortables_from_json
    assert scenario.warnings == warns


def test_sortables_failure(monkeypatch, scenario, fail_service_result):
    monkeypatch.setattr(
        FetchSortablesRunner,
        "run",
        lambda client, scen: fail_service_result(["sortable fetch failed"]),
    )

    with pytest.raises(ScenarioError):
        _ = scenario.sortables


# ------ custom_curves ------ #


@pytest.fixture(autouse=True)
def patch_custom_curves_from_json(monkeypatch):
    dummy = object()
    monkeypatch.setattr(CustomCurves, "from_json", staticmethod(lambda data: dummy))
    return dummy


def test_custom_curves_success(
    monkeypatch, patch_custom_curves_from_json, scenario, ok_service_result
):
    curves_data = [
        {"attached": True, "key": "interconnector_2_export"},
        {"attached": True, "key": "solar_pv_profile_1"},
        {"attached": False, "key": "wind_profile_1"},
    ]

    monkeypatch.setattr(
        FetchAllCurveDataRunner,
        "run",
        lambda client, scen: ok_service_result(curves_data),
    )

    coll = scenario.custom_curves
    assert coll is patch_custom_curves_from_json
    assert scenario._custom_curves is coll
    assert scenario.warnings == []


def test_custom_curves_with_warnings(
    monkeypatch, patch_custom_curves_from_json, scenario, ok_service_result
):
    curves_data = [{"attached": True, "key": "incomplete_curve"}]
    warns = ["some curves could not be loaded"]

    monkeypatch.setattr(
        FetchAllCurveDataRunner,
        "run",
        lambda client, scen: ok_service_result(curves_data, warns),
    )

    coll = scenario.custom_curves
    assert coll is patch_custom_curves_from_json
    assert scenario.warnings == warns


def test_custom_curves_failure(monkeypatch, scenario, fail_service_result):
    monkeypatch.setattr(
        FetchAllCurveDataRunner,
        "run",
        lambda client, scen: fail_service_result(["custom curves fetch failed"]),
    )

    with pytest.raises(ScenarioError):
        _ = scenario.custom_curves


# ------ Validation tests ------ #


def test_end_year_greater_than_start_year(minimal_scenario_metadata):
    """Test that end_year must be greater than start_year"""
    invalid_data = minimal_scenario_metadata.copy()
    invalid_data.update({"start_year": 2040, "end_year": 2030})

    with pytest.raises(ValueError, match="End year .* must be greater than start year"):
        Scenario.model_validate(invalid_data)


def test_to_dataframe(scenario):
    scenario = Scenario(id=scenario.id, area_code="nl2015", end_year=2050)
    dataframe = scenario.to_dataframe()

    assert dataframe[scenario.id]["end_year"] == 2050
