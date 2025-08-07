from unittest.mock import Mock
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
from pyetm.services.scenario_runners.update_sortables import UpdateSortablesRunner

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
    assert len(scenario.warnings) == 0


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
    assert len(scenario.warnings) == 0


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
    base_warnings = scenario.warnings.get_by_field("base")
    assert len(base_warnings) == 1
    assert base_warnings[0].message == warnings[0]


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
    assert len(scenario.warnings) == 0


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
    metadata_warnings = scenario.warnings.get_by_field("metadata")
    assert len(metadata_warnings) == 1
    assert metadata_warnings[0].message == warnings[0]


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
    assert len(scenario.warnings) == 0


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
    assert len(scenario.warnings) == 0


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
    metadata_warnings = scenario.warnings.get_by_field("metadata")
    assert len(metadata_warnings) == 1
    assert metadata_warnings[0].message == warns[0]


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

    scenario = Scenario.load(4)
    end_year_warnings = scenario.warnings.get_by_field("end_year")
    assert len(end_year_warnings) > 0
    assert any("Field required" in w.message for w in end_year_warnings)


# ------ version ------- #


def test_version_when_no_url_set(scenario):
    assert scenario.version == ""


def test_version_when_url_stable():
    scenario = Scenario(
        id=4,
        area_code="nl",
        end_year=2050,
        url="https://2025-01.engine.energytransitionmodel.com/api/v3/scenarios/4",
    )

    assert scenario.version == "2025-01"


def test_version_when_url_latest():
    scenario = Scenario(
        id=4,
        area_code="nl",
        end_year=2050,
        url="https://engine.energytransitionmodel.com/api/v3/scenarios/4",
    )

    assert scenario.version == "latest"


# ------- inputs ------- #


def test_inputs_success(monkeypatch, scenario, inputs_json, ok_service_result):
    monkeypatch.setattr(
        FetchInputsRunner, "run", lambda client, scen: ok_service_result(inputs_json)
    )

    coll = scenario.inputs
    assert scenario._inputs is coll
    assert len(scenario.warnings) == 0


def test_inputs_with_warnings(monkeypatch, inputs_json, scenario, ok_service_result):
    warns = ["parsed default with fallback"]

    monkeypatch.setattr(
        FetchInputsRunner,
        "run",
        lambda client, scen: ok_service_result(inputs_json, warns),
    )

    coll = scenario.inputs
    assert coll
    assert next(iter(coll)).key in inputs_json.keys()
    inputs_warnings = scenario.warnings.get_by_field("inputs")
    assert len(inputs_warnings) == 1
    assert inputs_warnings[0].message == warns[0]


def test_inputs_failure(monkeypatch, scenario, fail_service_result):
    monkeypatch.setattr(
        FetchInputsRunner,
        "run",
        lambda client, scen: fail_service_result(["input fetch failed"]),
    )

    with pytest.raises(ScenarioError):
        _ = scenario.inputs


def test_update_inputs_success(monkeypatch, inputs_json, scenario, ok_service_result):
    """Test successful inputs update"""
    input_updates = {
        list(inputs_json.keys())[0]: 42.5,
        list(inputs_json.keys())[1]: "diesel",
    }
    updated_data = {
        "scenario": {
            "id": scenario.id,
            "user_values": input_updates,
        }
    }
    scenario._inputs = Inputs.from_json(inputs_json)
    targeted_input = next(iter(scenario._inputs))

    monkeypatch.setattr(
        UpdateInputsRunner,
        "run",
        lambda client, scen, inputs: ok_service_result(updated_data),
    )

    # First there was no val set
    assert targeted_input.user is None

    result = scenario.update_user_values(input_updates)

    # Should not return anything (returns None)
    assert result is None
    assert len(scenario.warnings) == 0
    # Inputs were updated
    assert targeted_input.user == 42.5


def test_update_inputs_single_input(
    monkeypatch, scenario, ok_service_result, inputs_json
):
    """Test updating a single input"""
    # Set up a cached inputs object first
    scenario._inputs = Inputs.from_json(inputs_json)
    # First input should be the float_input
    targeted_input = next(iter(scenario._inputs))
    new_value = 80.0

    updated_data = {
        "scenario": {"id": scenario.id, "user_values": {targeted_input.key: new_value}}
    }

    monkeypatch.setattr(
        UpdateInputsRunner,
        "run",
        lambda client, scen, inputs: ok_service_result(updated_data),
    )

    # First there was no val set
    assert targeted_input.user is None

    # Now we set the val
    scenario.update_user_values({targeted_input.key: new_value})

    # Cache should be invalidated
    assert targeted_input.user == new_value
    assert len(scenario.warnings) == 0


def test_update_inputs_with_warnings(
    monkeypatch, scenario, inputs_json, ok_service_result
):
    """Test inputs update with warnings"""
    # Set up a cached inputs object first
    scenario._inputs = Inputs.from_json(inputs_json)

    updated_data = {"scenario": {"id": scenario.id}}
    warnings = ["Input validation warning"]

    monkeypatch.setattr(
        UpdateInputsRunner,
        "run",
        lambda client, scen, inputs: ok_service_result(updated_data, warnings),
    )

    scenario.update_user_values({"investment_costs_co2_ccs": 42.5})
    # This is not likely to occur so we don't log them
    assert len(scenario.warnings) == 0
    assert scenario._inputs


def test_update_inputs_failure(monkeypatch, scenario, inputs_json, fail_service_result):
    """Test inputs update failure"""
    scenario._inputs = Inputs.from_json(inputs_json)

    monkeypatch.setattr(
        UpdateInputsRunner,
        "run",
        lambda client, scen, inputs: fail_service_result(["422: Invalid input value"]),
    )

    with pytest.raises(ScenarioError, match="Could not update user values"):
        scenario.update_user_values({"invalid_input": "bad_value"})


def test_update_inputs_empty_dict(
    monkeypatch, scenario, ok_service_result, inputs_json
):
    """Test inputs update with empty dictionary"""
    scenario._inputs = Inputs.from_json(inputs_json)

    updated_data = {"scenario": {"id": scenario.id, "user_values": {}}}

    monkeypatch.setattr(
        UpdateInputsRunner,
        "run",
        lambda client, scen, inputs: ok_service_result(updated_data),
    )

    scenario.update_user_values({})
    assert len(scenario.warnings) == 0
    assert not scenario.user_values()


def test_update_inputs_preserves_existing_warnings(scenario, inputs_json):
    """Test that update_inputs preserves existing warnings on the scenario"""
    scenario.add_warning("queries", "Existing warning 1")
    scenario.add_warning("queries", "Existing warning 2")

    # Set up a cached inputs object first
    scenario._inputs = Inputs.from_json(inputs_json)

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
        scenario.update_user_values({"investment_costs_co2_ccs": 42})

        queries_warnings = scenario.warnings.get_by_field("queries")
        expected_messages = ["Existing warning 1", "Existing warning 2"]

        assert len(queries_warnings) == 2
        warning_messages = [w.message for w in queries_warnings]
        for expected_msg in expected_messages:
            assert expected_msg in warning_messages

    finally:
        # Restore original method
        pyetm.services.scenario_runners.update_inputs.UpdateInputsRunner.run = (
            original_run
        )


# ------ sortables ------ #


@pytest.fixture
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
    assert len(scenario.warnings) == 0


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
    assert len(scenario.warnings) > 0


def test_sortables_failure(monkeypatch, scenario, fail_service_result):
    monkeypatch.setattr(
        FetchSortablesRunner,
        "run",
        lambda client, scen: fail_service_result(["sortable fetch failed"]),
    )

    with pytest.raises(ScenarioError):
        _ = scenario.sortables


def test_set_sortables_from_dataframe(monkeypatch, scenario):
    import pandas as pd

    df = pd.DataFrame({"forecast_storage": [1, 2, 3], "heat_network_lt": [4, 5, None]})

    update_calls = []

    def mock_update_sortables(self, updates):
        update_calls.append(updates)

    monkeypatch.setattr(scenario.__class__, "update_sortables", mock_update_sortables)

    scenario.set_sortables_from_dataframe(df)

    expected = {
        "forecast_storage": [1, 2, 3],
        "heat_network_lt": [4, 5],
    }
    assert update_calls[0] == expected


def test_update_sortables(monkeypatch, scenario, ok_service_result):
    updates = {"forecast_storage": [1, 2, 3]}

    mock_sortables = Mock()
    mock_sortables.is_valid_update.return_value = {}
    mock_sortables.update = Mock()
    scenario._sortables = mock_sortables

    monkeypatch.setattr(
        UpdateSortablesRunner, "run", lambda *args, **kwargs: ok_service_result({})
    )

    scenario.update_sortables(updates)

    mock_sortables.is_valid_update.assert_called_once_with(updates)
    mock_sortables.update.assert_called_once_with(updates)


def test_update_sortables_validation_error(scenario):
    from pyetm.models.warnings import WarningCollector

    updates = {"nonexistent": [1, 2, 3]}

    mock_sortables = Mock()
    error_collector = WarningCollector.with_warning(
        "nonexistent", "Sortable does not exist"
    )
    mock_sortables.is_valid_update.return_value = {"nonexistent": error_collector}
    scenario._sortables = mock_sortables

    with pytest.raises(ScenarioError):
        scenario.update_sortables(updates)


def test_remove_sortables(monkeypatch, scenario, ok_service_result):
    sortable_names = ["forecast_storage", "hydrogen_supply"]

    mock_sortables = Mock()
    mock_sortables.update = Mock()
    scenario._sortables = mock_sortables

    monkeypatch.setattr(
        UpdateSortablesRunner, "run", lambda *args, **kwargs: ok_service_result({})
    )

    scenario.remove_sortables(sortable_names)

    expected_updates = {"forecast_storage": [], "hydrogen_supply": []}
    mock_sortables.update.assert_called_once_with(expected_updates)


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
    assert len(scenario.warnings) == 0


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
    custom_curves_warnings = scenario.warnings.get_by_field("custom_curves")
    assert len(custom_curves_warnings) == 1
    assert custom_curves_warnings[0].message == warns[0]


def test_custom_curves_failure(monkeypatch, scenario, fail_service_result):
    monkeypatch.setattr(
        FetchAllCustomCurveDataRunner,
        "run",
        lambda client, scen: fail_service_result(["custom curves fetch failed"]),
    )

    with pytest.raises(ScenarioError):
        _ = scenario.custom_curves


def test_to_dataframe(scenario):
    scenario = Scenario(id=scenario.id, area_code="nl2019", end_year=2050)
    dataframe = scenario.to_dataframe()

    assert dataframe[scenario.id]["end_year"] == 2050


# ------ Warnings tests ------ #


def test_scenario_warning_system_integration(scenario):
    """Test that the scenario properly integrates with the new warning system"""
    # Add some warnings
    scenario.add_warning("test_field", "Test warning message")
    scenario.add_warning("test_field", "Another warning")
    scenario.add_warning("other_field", "Different field warning", "error")

    # Check warning collector functionality
    assert len(scenario.warnings) == 3
    assert scenario.warnings.has_warnings("test_field")
    assert scenario.warnings.has_warnings("other_field")

    test_warnings = scenario.warnings.get_by_field("test_field")
    assert len(test_warnings) == 2

    other_warnings = scenario.warnings.get_by_field("other_field")
    assert len(other_warnings) == 1
    assert other_warnings[0].severity == "error"


def test_scenario_show_all_warnings(scenario, capsys):
    """Test the show_all_warnings method"""
    scenario.add_warning("test_field", "Test warning")

    scenario.show_all_warnings()

    captured = capsys.readouterr()
    assert f"Warnings for Scenario {scenario.id}" in captured.out
    assert "Scenario warnings:" in captured.out
    assert "Test warning" in captured.out


# ------ Update Custom Curves Tests ------ #


def test_scenario_update_custom_curves_success(monkeypatch, ok_service_result):
    """Test successful custom curves update"""
    from pyetm.models.custom_curves import CustomCurve, CustomCurves
    from pyetm.services.scenario_runners.update_custom_curves import UpdateCustomCurvesRunner
    from pyetm.models.warnings import WarningCollector
    import pandas as pd
    import numpy as np
    
    scenario = Scenario(id=12345, area_code="nl", end_year=2050)
    scenario._custom_curves = CustomCurves(curves=[])
    
    # Create valid custom curves (mock file data)
    curve = CustomCurve(key="test_curve", type="profile")
    custom_curves = CustomCurves(curves=[curve])
    
    # Mock validate_for_upload to return no errors
    def mock_validate():
        return {}
    
    # Mock UpdateCustomCurvesRunner to succeed
    def mock_runner(client, scenario, curves):
        return ok_service_result({
            "uploaded_curves": ["test_curve"],
            "total_curves": 1,
            "successful_uploads": 1
        })
    
    monkeypatch.setattr(custom_curves, "validate_for_upload", mock_validate)
    monkeypatch.setattr(UpdateCustomCurvesRunner, "run", mock_runner)
    
    # Should succeed without raising exception
    scenario.update_custom_curves(custom_curves)
    
    # Verify curve was added to scenario's curves
    assert len(scenario.custom_curves.curves) == 1
    assert scenario.custom_curves.curves[0].key == "test_curve"


def test_scenario_update_custom_curves_validation_error():
    """Test custom curves update with validation errors"""
    from pyetm.models.custom_curves import CustomCurve, CustomCurves
    from pyetm.models.warnings import WarningCollector
    
    scenario = Scenario(id=12345, area_code="nl", end_year=2050)
    
    # Create custom curves
    curve = CustomCurve(key="invalid_curve", type="profile")
    custom_curves = CustomCurves(curves=[curve])
    
    # Mock validate_for_upload to return validation errors
    def mock_validate():
        warning_collector = WarningCollector()
        warning_collector.add("invalid_curve", "Curve contains non-numeric values")
        return {"invalid_curve": warning_collector}
    
    custom_curves.validate_for_upload = mock_validate
    
    # Should raise ScenarioError due to validation failure
    with pytest.raises(ScenarioError) as exc_info:
        scenario.update_custom_curves(custom_curves)
    
    assert "Could not update custom curves" in str(exc_info.value)
    assert "invalid_curve" in str(exc_info.value)
    assert "Curve contains non-numeric values" in str(exc_info.value)


def test_scenario_update_custom_curves_runner_failure(monkeypatch, fail_service_result):
    """Test custom curves update with runner failure"""
    from pyetm.models.custom_curves import CustomCurve, CustomCurves
    from pyetm.services.scenario_runners.update_custom_curves import UpdateCustomCurvesRunner
    
    scenario = Scenario(id=12345, area_code="nl", end_year=2050)
    
    # Create valid custom curves
    curve = CustomCurve(key="test_curve", type="profile")
    custom_curves = CustomCurves(curves=[curve])
    
    # Mock validate_for_upload to return no errors
    def mock_validate():
        return {}
    
    # Mock UpdateCustomCurvesRunner to fail
    def mock_runner(client, scenario, curves):
        return fail_service_result(["HTTP 500: Internal server error"])
    
    monkeypatch.setattr(custom_curves, "validate_for_upload", mock_validate)
    monkeypatch.setattr(UpdateCustomCurvesRunner, "run", mock_runner)
    
    # Should raise ScenarioError due to runner failure
    with pytest.raises(ScenarioError) as exc_info:
        scenario.update_custom_curves(custom_curves)
    
    assert "Could not update custom curves" in str(exc_info.value)
    assert "HTTP 500: Internal server error" in str(exc_info.value)


def test_scenario_update_custom_curves_updates_existing_curve(monkeypatch, ok_service_result):
    """Test that updating existing curves replaces file_path"""
    from pyetm.models.custom_curves import CustomCurve, CustomCurves
    from pyetm.services.scenario_runners.update_custom_curves import UpdateCustomCurvesRunner
    from pathlib import Path
    
    scenario = Scenario(id=12345, area_code="nl", end_year=2050)
    
    # Set up scenario with existing curve
    existing_curve = CustomCurve(key="existing_curve", type="profile", file_path=Path("/old/path.csv"))
    scenario._custom_curves = CustomCurves(curves=[existing_curve])
    
    # Create new curves with same key but different file path
    new_curve = CustomCurve(key="existing_curve", type="profile", file_path=Path("/new/path.csv"))
    custom_curves = CustomCurves(curves=[new_curve])
    
    # Mock validate_for_upload to return no errors
    def mock_validate():
        return {}
    
    # Mock UpdateCustomCurvesRunner to succeed
    def mock_runner(client, scenario, curves):
        return ok_service_result({
            "uploaded_curves": ["existing_curve"],
            "total_curves": 1,
            "successful_uploads": 1
        })
    
    monkeypatch.setattr(custom_curves, "validate_for_upload", mock_validate)
    monkeypatch.setattr(UpdateCustomCurvesRunner, "run", mock_runner)
    
    # Update curves
    scenario.update_custom_curves(custom_curves)
    
    # Verify existing curve was updated with new file path
    assert len(scenario.custom_curves.curves) == 1
    updated_curve = scenario.custom_curves.curves[0]
    assert updated_curve.key == "existing_curve"
    assert updated_curve.file_path == Path("/new/path.csv")


def test_scenario_update_custom_curves_adds_new_curve(monkeypatch, ok_service_result):
    """Test that new curves are added to scenario's curves collection"""
    from pyetm.models.custom_curves import CustomCurve, CustomCurves
    from pyetm.services.scenario_runners.update_custom_curves import UpdateCustomCurvesRunner
    from pathlib import Path
    
    scenario = Scenario(id=12345, area_code="nl", end_year=2050)
    
    # Set up scenario with one existing curve
    existing_curve = CustomCurve(key="existing_curve", type="profile", file_path=Path("/old/path.csv"))
    scenario._custom_curves = CustomCurves(curves=[existing_curve])
    
    # Create new curve with different key
    new_curve = CustomCurve(key="new_curve", type="availability", file_path=Path("/new/path.csv"))
    custom_curves = CustomCurves(curves=[new_curve])
    
    # Mock validate_for_upload to return no errors
    def mock_validate():
        return {}
    
    # Mock UpdateCustomCurvesRunner to succeed
    def mock_runner(client, scenario, curves):
        return ok_service_result({
            "uploaded_curves": ["new_curve"],
            "total_curves": 1,
            "successful_uploads": 1
        })
    
    monkeypatch.setattr(custom_curves, "validate_for_upload", mock_validate)
    monkeypatch.setattr(UpdateCustomCurvesRunner, "run", mock_runner)
    
    # Update curves
    scenario.update_custom_curves(custom_curves)
    
    # Verify both curves exist
    assert len(scenario.custom_curves.curves) == 2
    curve_keys = {curve.key for curve in scenario.custom_curves.curves}
    assert curve_keys == {"existing_curve", "new_curve"}


def test_scenario_update_custom_curves_multiple_validation_errors():
    """Test custom curves update with multiple validation errors"""
    from pyetm.models.custom_curves import CustomCurve, CustomCurves
    from pyetm.models.warnings import WarningCollector
    
    scenario = Scenario(id=12345, area_code="nl", end_year=2050)
    
    # Create custom curves
    curves = [
        CustomCurve(key="curve1", type="profile"),
        CustomCurve(key="curve2", type="availability")
    ]
    custom_curves = CustomCurves(curves=curves)
    
    # Mock validate_for_upload to return multiple validation errors
    def mock_validate():
        errors = {}
        
        # Curve1 errors
        curve1_warnings = WarningCollector()
        curve1_warnings.add("curve1", "Wrong length")
        curve1_warnings.add("curve1", "Non-numeric values")
        errors["curve1"] = curve1_warnings

        # Curve2 errors
        curve2_warnings = WarningCollector()
        curve2_warnings.add("curve2", "No data available")
        errors["curve2"] = curve2_warnings

        return errors
    
    custom_curves.validate_for_upload = mock_validate
    
    # Should raise ScenarioError with all validation errors
    with pytest.raises(ScenarioError) as exc_info:
        scenario.update_custom_curves(custom_curves)
    
    error_message = str(exc_info.value)
    assert "Could not update custom curves" in error_message
    assert "curve1" in error_message
    assert "curve2" in error_message
    assert "Wrong length" in error_message
    assert "Non-numeric values" in error_message
    assert "No data available" in error_message
