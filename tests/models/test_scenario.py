import pytest
from pyetm.clients.base_client import BaseClient
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

# ------ New scenario ------ #


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
    """Test successful metadata update."""
    updated_data = {"scenario": {"id": scenario.id, "end_year": 2050, "private": True}}

    monkeypatch.setattr(
        UpdateMetadataRunner,
        "run",
        lambda client, scen, metadata: ok_service_result(updated_data),
    )

    result = scenario.update_metadata(end_year=2050, private=True, custom_field="value")

    assert result == updated_data
    assert scenario.end_year == 2050
    assert scenario.private == True


def test_update_metadata_with_warnings(monkeypatch, scenario, ok_service_result):
    """Test metadata update with warnings."""
    updated_data = {"scenario": {"id": scenario.id, "private": True}}
    warnings = ["Field 'id' cannot be updated directly"]

    monkeypatch.setattr(
        UpdateMetadataRunner,
        "run",
        lambda client, scen, metadata: ok_service_result(updated_data, warnings),
    )

    result = scenario.update_metadata(private=True, id=999)

    assert result == updated_data
    assert scenario.warnings == warnings


def test_update_metadata_failure(monkeypatch, scenario, fail_service_result):
    """Test metadata update failure raises ScenarioError."""
    monkeypatch.setattr(
        UpdateMetadataRunner,
        "run",
        lambda client, scen, metadata: fail_service_result(["422: Validation Error"]),
    )

    with pytest.raises(ScenarioError, match="Could not update metadata"):
        scenario.update_metadata(end_year="invalid")


def test_update_metadata_empty_kwargs(monkeypatch, scenario, ok_service_result):
    """Test metadata update with no arguments."""
    updated_data = {"scenario": {"id": scenario.id}}

    monkeypatch.setattr(
        UpdateMetadataRunner,
        "run",
        lambda client, scen, metadata: ok_service_result(updated_data),
    )

    result = scenario.update_metadata()

    assert result == updated_data
    assert scenario.warnings == []


def test_update_metadata_partial_scenario_update(
    monkeypatch, scenario, ok_service_result
):
    """Test scenario object update with partial data."""
    original_end_year = scenario.end_year
    updated_data = {
        "scenario": {
            "metadata": {"updated_key": "updated_value"}
            # Note: no end_year field in response
        }
    }

    monkeypatch.setattr(
        UpdateMetadataRunner,
        "run",
        lambda client, scen, metadata: ok_service_result(updated_data),
    )

    result = scenario.update_metadata(some_field="value")

    # Only metadata should be updated, end_year should remain unchanged
    assert scenario.metadata == {"updated_key": "updated_value"}
    assert scenario.end_year == original_end_year


def test_update_metadata_no_scenario_data(monkeypatch, scenario, ok_service_result):
    """Test update when response contains no scenario data."""
    original_metadata = getattr(scenario, "metadata", None)
    updated_data = {"other_data": "value"}  # No scenario key

    monkeypatch.setattr(
        UpdateMetadataRunner,
        "run",
        lambda client, scen, metadata: ok_service_result(updated_data),
    )

    result = scenario.update_metadata(some_field="value")

    # Scenario object should remain unchanged
    assert getattr(scenario, "metadata", None) == original_metadata
    assert result == updated_data


def test_update_metadata_ignores_nonexistent_fields(
    monkeypatch, scenario, ok_service_result
):
    """Test update when scenario doesn't have a field from response."""
    original_end_year = scenario.end_year
    updated_data = {"scenario": {"nonexistent_field": "value", "end_year": 2060}}

    monkeypatch.setattr(
        UpdateMetadataRunner,
        "run",
        lambda client, scen, metadata: ok_service_result(updated_data),
    )

    result = scenario.update_metadata(some_field="value")

    # Only existing fields should be updated
    assert scenario.end_year == 2060
    assert not hasattr(scenario, "nonexistent_field")


def test_update_metadata_runner_receives_kwargs(
    monkeypatch, scenario, ok_service_result
):
    """Test that runner receives the kwargs as metadata parameter."""
    updated_data = {"scenario": {"id": scenario.id}}
    captured_metadata = None

    def mock_run(client, scen, metadata):
        nonlocal captured_metadata
        captured_metadata = metadata
        return ok_service_result(updated_data)

    monkeypatch.setattr(UpdateMetadataRunner, "run", mock_run)

    scenario.update_metadata(end_year=2050, private=True, custom_field="custom_value")

    assert captured_metadata == {
        "end_year": 2050,
        "private": True,
        "custom_field": "custom_value",
    }


def test_update_metadata_runner_receives_scenario_object(
    monkeypatch, scenario, ok_service_result
):
    """Test that runner receives the scenario object."""
    updated_data = {"scenario": {"id": scenario.id}}
    captured_scenario = None

    def mock_run(client, scen, metadata):
        nonlocal captured_scenario
        captured_scenario = scen
        return ok_service_result(updated_data)

    monkeypatch.setattr(UpdateMetadataRunner, "run", mock_run)

    scenario.update_metadata(test="value")

    assert captured_scenario is scenario


def test_update_metadata_uses_base_client(monkeypatch, scenario, ok_service_result):
    """Test that runner is called with BaseClient instance."""
    updated_data = {"scenario": {"id": scenario.id}}
    captured_client = None

    def mock_run(client, scen, metadata):
        nonlocal captured_client
        captured_client = client
        return ok_service_result(updated_data)

    monkeypatch.setattr(UpdateMetadataRunner, "run", mock_run)

    scenario.update_metadata(test="value")

    from pyetm.clients.base_client import BaseClient

    assert isinstance(captured_client, BaseClient)


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


# ------ Validation tests ------ #


# def test_end_year_greater_than_start_year(minimal_scenario_metadata):
#     """Test that end_year must be greater than start_year"""
#     invalid_data = minimal_scenario_metadata.copy()
#     invalid_data.update({"start_year": 2040, "end_year": 2030})

#     with pytest.raises(ValueError, match="End year .* must be greater than start year"):
#         Scenario.model_validate(invalid_data)


def test_to_dataframe(scenario):
    scenario = Scenario(id=scenario.id, area_code="nl2019", end_year=2050)
    dataframe = scenario.to_dataframe()

    assert dataframe[scenario.id]["end_year"] == 2050
