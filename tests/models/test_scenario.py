from pydantic import ValidationError
import pytest
from pyetm.models.inputs import Inputs
from pyetm.models.custom_curves import CustomCurves
from pyetm.models.scenario import Scenario, ScenarioError
from pyetm.services.scenario_runners.fetch_custom_curves import (
    FetchAllCustomCurveDataRunner,
)
from pyetm.services.scenario_runners.fetch_inputs import FetchInputsRunner
from pyetm.services.scenario_runners.fetch_metadata import FetchMetadataRunner
from pyetm.services.scenario_runners.fetch_sortables import FetchSortablesRunner
from pyetm.models.sortables import Sortables
from pyetm.services.scenario_runners.create_scenario import CreateScenarioRunner
from pyetm.services.scenario_runners.update_metadata import UpdateMetadataRunner
from pyetm.services.scenario_runners.update_inputs import UpdateInputsRunner

# ------ New ------ #


def test_new_scenario_success_minimal(monkeypatch, ok_service_result):
    """Test successful scenario creation with minimal required fields"""
    created_scenario_data = {
        "id": 12345,
        "area_code": "nl",
        "end_year": 2050,
        "private": False,
        "created_at": "2019-01-01T00:00:00Z",
    }

    monkeypatch.setattr(
        CreateScenarioRunner,
        "run",
        lambda client, data: ok_service_result(created_scenario_data),
    )

    scenario = Scenario.new("nl", 2050)
    assert scenario.id == 12345
    assert scenario.area_code == "nl"
    assert scenario.end_year == 2050
    assert scenario.private is False
    assert scenario.warnings == []


def test_new_scenario_success_with_kwargs(monkeypatch, ok_service_result):
    """Test successful scenario creation with optional fields"""
    created_scenario_data = {
        "id": 12346,
        "area_code": "nl",
        "end_year": 2050,
        "private": True,
        "start_year": 2019,
        "source": "pyetm",
        "metadata": {"description": "Test scenario"},
    }

    monkeypatch.setattr(
        CreateScenarioRunner,
        "run",
        lambda client, data: ok_service_result(created_scenario_data),
    )

    scenario = Scenario.new(
        area_code="nl",
        end_year=2050,
        private=True,
        start_year=2019,
        source="pyetm",
        metadata={"description": "Test scenario"},
    )
    assert scenario.id == 12346
    assert scenario.area_code == "nl"
    assert scenario.private is True
    assert scenario.start_year == 2019
    assert scenario.source == "pyetm"
    assert scenario.warnings == []


def test_new_scenario_with_warnings(monkeypatch, ok_service_result):
    """Test scenario creation with warnings"""
    created_scenario_data = {"id": 12347, "area_code": "nl", "end_year": 2050}
    warnings = ["Ignoring invalid field for scenario creation: 'invalid_field'"]

    monkeypatch.setattr(
        CreateScenarioRunner,
        "run",
        lambda client, data: ok_service_result(created_scenario_data, warnings),
    )

    scenario = Scenario.new("nl", 2050, invalid_field="should_be_ignored")
    assert scenario.id == 12347
    assert scenario.warnings == warnings


def test_new_scenario_failure(monkeypatch, fail_service_result):
    """Test scenario creation failure"""
    monkeypatch.setattr(
        CreateScenarioRunner,
        "run",
        lambda client, data: fail_service_result(["Missing required field: area_code"]),
    )

    with pytest.raises(ScenarioError, match="Could not create scenario"):
        Scenario.new("", 2050)  # Invalid area_code


# ------ update_metadata ------ #


def test_update_metadata_success(monkeypatch, scenario, ok_service_result):
    """Test successful metadata update"""
    updated_data = {
        "scenario": {
            "id": scenario.id,
            "end_year": 2060,
            "private": True,
            "source": "pyetm_update",
        }
    }

    monkeypatch.setattr(
        UpdateMetadataRunner,
        "run",
        lambda client, scen, metadata: ok_service_result(updated_data),
    )

    metadata_updates = {"end_year": 2060, "private": True, "source": "pyetm_update"}

    result = scenario.update_metadata(metadata_updates)
    assert result == updated_data
    assert scenario.warnings == []


def test_update_metadata_with_warnings(monkeypatch, scenario, ok_service_result):
    """Test metadata update with warnings"""
    updated_data = {"scenario": {"id": scenario.id, "private": True}}
    warnings = ["Ignoring non-updatable metadata field: 'id'"]

    monkeypatch.setattr(
        UpdateMetadataRunner,
        "run",
        lambda client, scen, metadata: ok_service_result(updated_data, warnings),
    )

    metadata_updates = {"private": True, "id": 999}  # Should generate warning

    result = scenario.update_metadata(metadata_updates)
    assert result == updated_data
    assert scenario.warnings == warnings


def test_update_metadata_failure(monkeypatch, scenario, fail_service_result):
    """Test metadata update failure"""
    monkeypatch.setattr(
        UpdateMetadataRunner,
        "run",
        lambda client, scen, metadata: fail_service_result(["422: Validation Error"]),
    )

    metadata_updates = {"end_year": "invalid"}

    with pytest.raises(ScenarioError, match="Could not update metadata"):
        scenario.update_metadata(metadata_updates)


def test_update_metadata_empty_dict(monkeypatch, scenario, ok_service_result):
    """Test metadata update with empty dictionary"""
    updated_data = {"scenario": {"id": scenario.id}}

    monkeypatch.setattr(
        UpdateMetadataRunner,
        "run",
        lambda client, scen, metadata: ok_service_result(updated_data),
    )

    result = scenario.update_metadata({})
    assert result == updated_data
    assert scenario.warnings == []


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
    print(Scenario.load(4).warnings)
    assert "end_year: Field required" in Scenario.load(4).warnings


# ------ version ------- #


def test_version_when_no_url_set(scenario):
    assert scenario.version == ""


def test_version_when_url_stable():
    scenario = Scenario(
        id=4, url="https://2025-01.engine.energytransitionmodel.com/api/v3/scenarios/4"
    )

    assert scenario.version == "2025-01"


def test_version_when_url_latest():
    scenario = Scenario(
        id=4, url="https://engine.energytransitionmodel.com/api/v3/scenarios/4"
    )

    assert scenario.version == "latest"


# ------- inputs ------- #


@pytest.fixture(autouse=True)
def patch_input_from_json(monkeypatch):
    dummy = object()
    monkeypatch.setattr(Inputs, "from_json", staticmethod(lambda data: dummy))
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


def test_update_inputs_success(monkeypatch, scenario, ok_service_result):
    """Test successful inputs update"""
    updated_data = {
        "scenario": {
            "id": scenario.id,
            "user_values": {"input_key_1": 42.5, "input_key_2": 100.0},
        }
    }

    monkeypatch.setattr(
        UpdateInputsRunner,
        "run",
        lambda client, scen, inputs: ok_service_result(updated_data),
    )

    input_updates = {"input_key_1": 42.5, "input_key_2": 100.0}

    # Should not return anything (returns None)
    result = scenario.update_inputs(input_updates)
    assert result is None
    assert scenario.warnings == []
    # Cache should be invalidated
    assert scenario._inputs is None


def test_update_inputs_single_input(monkeypatch, scenario, ok_service_result):
    """Test updating a single input"""
    updated_data = {
        "scenario": {"id": scenario.id, "user_values": {"co_firing_biocoal_share": 80}}
    }

    # Set up a cached inputs object first
    scenario._inputs = "cached_inputs_object"

    monkeypatch.setattr(
        UpdateInputsRunner,
        "run",
        lambda client, scen, inputs: ok_service_result(updated_data),
    )

    scenario.update_inputs({"co_firing_biocoal_share": 80})

    # Cache should be invalidated
    assert scenario._inputs is None
    assert scenario.warnings == []


def test_update_inputs_with_warnings(monkeypatch, scenario, ok_service_result):
    """Test inputs update with warnings"""
    updated_data = {"scenario": {"id": scenario.id}}
    warnings = ["Input validation warning"]

    monkeypatch.setattr(
        UpdateInputsRunner,
        "run",
        lambda client, scen, inputs: ok_service_result(updated_data, warnings),
    )

    scenario.update_inputs({"some_input": 42.5})
    assert scenario.warnings == warnings
    assert scenario._inputs is None


def test_update_inputs_failure(monkeypatch, scenario, fail_service_result):
    """Test inputs update failure"""
    monkeypatch.setattr(
        UpdateInputsRunner,
        "run",
        lambda client, scen, inputs: fail_service_result(["422: Invalid input value"]),
    )

    with pytest.raises(ScenarioError, match="Could not update inputs"):
        scenario.update_inputs({"invalid_input": "bad_value"})


def test_update_inputs_empty_dict(monkeypatch, scenario, ok_service_result):
    """Test inputs update with empty dictionary"""
    updated_data = {"scenario": {"id": scenario.id, "user_values": {}}}

    monkeypatch.setattr(
        UpdateInputsRunner,
        "run",
        lambda client, scen, inputs: ok_service_result(updated_data),
    )

    scenario.update_inputs({})
    assert scenario.warnings == []
    assert scenario._inputs is None


def test_update_inputs_preserves_existing_warnings(scenario):
    """Test that update_inputs preserves existing warnings on the scenario"""
    scenario.add_warning("Existing warning 1")
    scenario.add_warning("Existing warning 2")

    # Mock a successful update with new warnings
    def mock_runner_run(client, scen, inputs):
        from pyetm.services.service_result import ServiceResult

        return ServiceResult.ok(
            data={"scenario": {"id": scen.id}}, errors=["New warning from update"]
        )

    import pyetm.services.scenario_runners.update_inputs

    original_run = pyetm.services.scenario_runners.update_inputs.UpdateInputsRunner.run
    pyetm.services.scenario_runners.update_inputs.UpdateInputsRunner.run = staticmethod(
        mock_runner_run
    )

    try:
        scenario.update_inputs({"test_input": 42})

        # Should have both existing and new warnings
        expected_warnings = [
            "Existing warning 1",
            "Existing warning 2",
            "New warning from update",
        ]
        assert scenario.warnings == expected_warnings
    finally:
        # Restore original method
        pyetm.services.scenario_runners.update_inputs.UpdateInputsRunner.run = (
            original_run
        )


# ------ sortables ------ #


@pytest.fixture(autouse=True)
def patch_sortables_from_json(monkeypatch):
    dummy = object()
    monkeypatch.setattr(Sortables, "from_json", staticmethod(lambda data: dummy))
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
        FetchAllCustomCurveDataRunner,
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
        FetchAllCustomCurveDataRunner,
        "run",
        lambda client, scen: ok_service_result(curves_data, warns),
    )

    coll = scenario.custom_curves
    assert coll is patch_custom_curves_from_json
    assert scenario.warnings == warns


def test_custom_curves_failure(monkeypatch, scenario, fail_service_result):
    monkeypatch.setattr(
        FetchAllCustomCurveDataRunner,
        "run",
        lambda client, scen: fail_service_result(["custom curves fetch failed"]),
    )

    with pytest.raises(ScenarioError):
        _ = scenario.custom_curves


def test_to_df(scenario):
    scenario = Scenario(id=scenario.id, area_code="nl2019", end_year=2050)
    dataframe = scenario.to_df()

    assert dataframe[scenario.id]["end_year"] == 2050
