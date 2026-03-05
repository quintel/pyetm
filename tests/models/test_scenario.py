from unittest.mock import Mock
import pytest
from pyetm.models.inputs import Inputs
from pyetm.models.custom_curves import CustomCurves
from pyetm.models.session import Session, ScenarioError
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
from pyetm.services.scenario_runners.copy_scenario import CopyScenarioRunner
from pyetm.services.scenario_runners.break_preset_link import BreakPresetLinkRunner
from pyetm.services.scenario_runners.interpolate_scenarios import (
    InterpolateScenariosRunner,
)


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

    scenario = Session.new("nl", 2050)
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
    }

    monkeypatch.setattr(
        CreateScenarioRunner,
        "run",
        lambda client, data: ok_service_result(created_scenario_data),
    )

    scenario = Session.new(
        area_code="nl",
        end_year=2050,
        private=True,
        start_year=2019,
        source="pyetm",
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

    scenario = Session.new("nl", 2050, invalid_field="should_be_ignored")
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
        Session.new("", 2050)  # Invalid area_code


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

    scenario = Session.load(1)
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

    scenario = Session.load(2)
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
        Session.load(3)


def test_load_missing_required_field(monkeypatch, ok_service_result):
    """Test scenario load fails when required fields are missing"""
    incomplete_data = {"id": 4}  # Missing end_year and area_code

    monkeypatch.setattr(
        FetchMetadataRunner,
        "run",
        lambda client, stub: ok_service_result(incomplete_data),
    )

    scenario = Session.load(4)
    end_year_warnings = scenario.warnings.get_by_field("end_year")
    assert len(end_year_warnings) > 0
    assert any("Field required" in w.message for w in end_year_warnings)


# ------ version ------- #


def test_version_when_no_url_set(scenario):
    assert scenario.version == ""


def test_version_when_url_stable():
    scenario = Session(
        id=4,
        area_code="nl",
        end_year=2050,
        url="https://2025-01.engine.energytransitionmodel.com/api/v3/scenarios/4",
    )

    assert scenario.version == "2025-01"


def test_version_when_url_latest():
    scenario = Session(
        id=4,
        area_code="nl",
        end_year=2050,
        url="https://engine.energytransitionmodel.com/api/v3/scenarios/4",
    )

    assert scenario.version == "latest"


# ------- inputs ------- #


def test_inputs_success(monkeypatch, scenario, inputs_json, ok_service_result):
    monkeypatch.setattr(
        FetchInputsRunner,
        "run",
        lambda client, scen, defaults=None: ok_service_result(inputs_json),
    )

    coll = scenario.inputs
    assert scenario._inputs is coll
    assert len(scenario.warnings) == 0


def test_inputs_with_warnings(monkeypatch, inputs_json, scenario, ok_service_result):
    warns = ["parsed default with fallback"]

    monkeypatch.setattr(
        FetchInputsRunner,
        "run",
        lambda client, scen, defaults=None: ok_service_result(inputs_json, warns),
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
        lambda client, scen, defaults=None: fail_service_result(["input fetch failed"]),
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

    def mock_update_sortables(self, updates, skip_upload=False):
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
        lambda client, scen, **kwargs: ok_service_result(curves_data),
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
        lambda client, scen, **kwargs: ok_service_result(curves_data, warns),
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
        lambda client, scen, **kwargs: fail_service_result(["custom curves fetch failed"]),
    )

    with pytest.raises(ScenarioError):
        _ = scenario.custom_curves


def test_to_dataframe(scenario):
    scenario = Session(id=scenario.id, area_code="nl2019", end_year=2050)
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
    from pyetm.services.scenario_runners.update_custom_curves import (
        UpdateCustomCurvesRunner,
    )
    from pyetm.models.warnings import WarningCollector
    import pandas as pd
    import numpy as np

    scenario = Session(id=12345, area_code="nl", end_year=2050)
    scenario._custom_curves = CustomCurves(curves=[])

    # Create valid custom curves (mock file data)
    curve = CustomCurve(key="test_curve", type="profile")
    custom_curves = CustomCurves(curves=[curve])

    # Mock validate_for_upload to return no errors
    def mock_validate():
        return {}

    # Mock UpdateCustomCurvesRunner to succeed
    def mock_runner(client, scenario, curves):
        return ok_service_result(
            {
                "uploaded_curves": ["test_curve"],
                "total_curves": 1,
                "successful_uploads": 1,
            }
        )

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

    scenario = Session(id=12345, area_code="nl", end_year=2050)

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
    from pyetm.services.scenario_runners.update_custom_curves import (
        UpdateCustomCurvesRunner,
    )

    scenario = Session(id=12345, area_code="nl", end_year=2050)

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


def test_scenario_update_custom_curves_updates_existing_curve(
    monkeypatch, ok_service_result
):
    """Test that updating existing curves replaces file_path"""
    from pyetm.models.custom_curves import CustomCurve, CustomCurves
    from pyetm.services.scenario_runners.update_custom_curves import (
        UpdateCustomCurvesRunner,
    )
    from pathlib import Path

    scenario = Session(id=12345, area_code="nl", end_year=2050)

    # Set up scenario with existing curve
    existing_curve = CustomCurve(
        key="existing_curve", type="profile", file_path=Path("/old/path.csv")
    )
    scenario._custom_curves = CustomCurves(curves=[existing_curve])

    # Create new curves with same key but different file path
    new_curve = CustomCurve(
        key="existing_curve", type="profile", file_path=Path("/new/path.csv")
    )
    custom_curves = CustomCurves(curves=[new_curve])

    # Mock validate_for_upload to return no errors
    def mock_validate():
        return {}

    # Mock UpdateCustomCurvesRunner to succeed
    def mock_runner(client, scenario, curves):
        return ok_service_result(
            {
                "uploaded_curves": ["existing_curve"],
                "total_curves": 1,
                "successful_uploads": 1,
            }
        )

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
    from pyetm.services.scenario_runners.update_custom_curves import (
        UpdateCustomCurvesRunner,
    )
    from pathlib import Path

    scenario = Session(id=12345, area_code="nl", end_year=2050)

    # Set up scenario with one existing curve
    existing_curve = CustomCurve(
        key="existing_curve", type="profile", file_path=Path("/old/path.csv")
    )
    scenario._custom_curves = CustomCurves(curves=[existing_curve])

    # Create new curve with different key
    new_curve = CustomCurve(
        key="new_curve", type="availability", file_path=Path("/new/path.csv")
    )
    custom_curves = CustomCurves(curves=[new_curve])

    # Mock validate_for_upload to return no errors
    def mock_validate():
        return {}

    # Mock UpdateCustomCurvesRunner to succeed
    def mock_runner(client, scenario, curves):
        return ok_service_result(
            {
                "uploaded_curves": ["new_curve"],
                "total_curves": 1,
                "successful_uploads": 1,
            }
        )

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

    scenario = Session(id=12345, area_code="nl", end_year=2050)

    # Create custom curves
    curves = [
        CustomCurve(key="curve1", type="profile"),
        CustomCurve(key="curve2", type="availability"),
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


# ------ copy ------ #


def test_copy_scenario_success_minimal(monkeypatch, ok_service_result, dummy_scenario):
    """Test successful scenario copy with no overrides"""
    copied_scenario_data = {
        "id": 67890,
        "area_code": "nl",
        "end_year": 2050,
        "private": False,
        "title": "Copy of Original Scenario",
    }

    monkeypatch.setattr(
        CopyScenarioRunner,
        "run",
        lambda client, scenario_id, overrides: ok_service_result(copied_scenario_data),
    )

    original = dummy_scenario(12345)
    scenario = original.copy_with_preset()
    assert scenario.id == 67890
    assert scenario.area_code == "nl"
    assert scenario.end_year == 2050
    assert scenario.title == "Copy of Original Scenario"
    assert len(scenario.warnings) == 0


def test_copy_scenario_with_title_override(
    monkeypatch, ok_service_result, dummy_scenario
):
    """Test successful scenario copy with title override"""
    copied_scenario_data = {
        "id": 67891,
        "area_code": "nl",
        "end_year": 2050,
        "title": "My Custom Copy",
    }

    monkeypatch.setattr(
        CopyScenarioRunner,
        "run",
        lambda client, scenario_id, overrides: ok_service_result(copied_scenario_data),
    )

    original = dummy_scenario(12345)
    scenario = original.copy_with_preset(title="My Custom Copy")
    assert scenario.id == 67891
    assert scenario.title == "My Custom Copy"
    assert len(scenario.warnings) == 0


def test_copy_scenario_with_multiple_overrides(
    monkeypatch, ok_service_result, dummy_scenario
):
    """Test successful scenario copy with multiple overrides"""
    copied_scenario_data = {
        "id": 67892,
        "area_code": "de",
        "end_year": 2040,
        "private": True,
        "title": "Private Copy",
        "source": "test",
    }

    monkeypatch.setattr(
        CopyScenarioRunner,
        "run",
        lambda client, scenario_id, overrides: ok_service_result(copied_scenario_data),
    )

    original = dummy_scenario(12345)
    scenario = original.copy_with_preset(
        title="Private Copy", private=True, source="test"
    )
    assert scenario.id == 67892
    assert scenario.title == "Private Copy"
    assert scenario.private is True
    assert scenario.source == "test"
    assert len(scenario.warnings) == 0


def test_copy_scenario_with_warnings(monkeypatch, ok_service_result, dummy_scenario):
    """Test scenario copy with warnings"""
    copied_scenario_data = {"id": 67893, "area_code": "nl", "end_year": 2050}
    warnings = ["Ignoring invalid field for scenario copy: 'invalid_field'"]

    monkeypatch.setattr(
        CopyScenarioRunner,
        "run",
        lambda client, scenario_id, overrides: ok_service_result(
            copied_scenario_data, warnings
        ),
    )

    original = dummy_scenario(12345)
    scenario = original.copy_with_preset(invalid_field="should_be_ignored")
    assert scenario.id == 67893
    base_warnings = scenario.warnings.get_by_field("base")
    assert len(base_warnings) == 1
    assert base_warnings[0].message == warnings[0]


def test_copy_scenario_failure(monkeypatch, fail_service_result, dummy_scenario):
    """Test scenario copy failure"""
    monkeypatch.setattr(
        CopyScenarioRunner,
        "run",
        lambda client, scenario_id, overrides: fail_service_result(
            ["Scenario not found"]
        ),
    )

    original = dummy_scenario(99999)
    with pytest.raises(ScenarioError, match="Failed to copy scenario"):
        original.copy_with_preset()


def test_copy_scenario_with_preset_scenario_id(
    monkeypatch, ok_service_result, dummy_scenario
):
    """Test that template is visible in copied scenarios"""
    copied_scenario_data = {
        "id": 67894,
        "area_code": "nl",
        "end_year": 2050,
        "template": 12345,
    }

    monkeypatch.setattr(
        CopyScenarioRunner,
        "run",
        lambda client, scenario_id, overrides: ok_service_result(copied_scenario_data),
    )

    original = dummy_scenario(12345)
    scenario = original.copy_with_preset()
    assert scenario.id == 67894
    assert scenario.template == 12345
    assert len(scenario.warnings) == 0


def test_copy_scenario_deep_copy_success(
    monkeypatch, ok_service_result, dummy_scenario
):
    """Test successful copy that breaks the preset link"""
    copied_scenario_data = {
        "id": 67894,
        "area_code": "nl",
        "end_year": 2050,
        "template": 12345,  # Initially linked
    }

    break_link_response = {
        "scenario": {
            "id": 67894,
            "area_code": "nl",
            "end_year": 2050,
            "template": None,  # Link broken
        }
    }

    # Track calls to verify both runners are called
    calls = []

    def mock_copy_run(client, scenario_id, overrides):
        calls.append(("copy", scenario_id))
        return ok_service_result(copied_scenario_data)

    def mock_break_link_run(client, scenario):
        calls.append(("break_link", scenario.id))
        return ok_service_result(break_link_response)

    monkeypatch.setattr(CopyScenarioRunner, "run", mock_copy_run)
    monkeypatch.setattr(BreakPresetLinkRunner, "run", mock_break_link_run)

    original = dummy_scenario(12345)
    scenario = original.copy()

    # Verify both operations were called
    assert len(calls) == 2
    assert calls[0] == ("copy", 12345)
    assert calls[1] == ("break_link", 67894)

    # Verify the scenario is correct and preset link was broken
    assert scenario.id == 67894
    assert scenario.template is None
    assert len(scenario.warnings) == 0


def test_copy_scenario_deep_copy_break_link_failure(
    monkeypatch, ok_service_result, fail_service_result, dummy_scenario
):
    """Test copy when breaking the preset link fails"""
    copied_scenario_data = {
        "id": 67895,
        "area_code": "nl",
        "end_year": 2050,
    }

    monkeypatch.setattr(
        CopyScenarioRunner,
        "run",
        lambda client, scenario_id, overrides: ok_service_result(copied_scenario_data),
    )

    monkeypatch.setattr(
        BreakPresetLinkRunner,
        "run",
        lambda client, scenario: fail_service_result(["Cannot modify scenario"]),
    )

    original = dummy_scenario(12345)
    with pytest.raises(
        ScenarioError, match="Copied scenario but failed to break template link"
    ):
        original.copy()


def test_copy_scenario_deep_false_doesnt_break_link(
    monkeypatch, ok_service_result, dummy_scenario
):
    """Test that copy_with_preset() doesn't call BreakPresetLinkRunner"""
    copied_scenario_data = {
        "id": 67896,
        "area_code": "nl",
        "end_year": 2050,
    }

    copy_called = []
    break_link_called = []

    def mock_copy_run(client, scenario_id, overrides):
        copy_called.append(True)
        return ok_service_result(copied_scenario_data)

    def mock_break_link_run(client, scenario):
        break_link_called.append(True)
        return ok_service_result({})

    monkeypatch.setattr(CopyScenarioRunner, "run", mock_copy_run)
    monkeypatch.setattr(BreakPresetLinkRunner, "run", mock_break_link_run)

    original = dummy_scenario(12345)
    scenario = original.copy_with_preset()

    # Verify only copy was called, not break_link
    assert len(copy_called) == 1
    assert len(break_link_called) == 0
    assert scenario.id == 67896




# ------ interpolate ------ #


def test_interpolate_success_two_scenarios(
    monkeypatch, ok_service_result, dummy_scenario
):
    """Test successful batch interpolation with two scenarios"""
    interpolated_data = [
        {
            "id": 88881,
            "area_code": "nl",
            "end_year": 2040,
            "start_year": 2023,
            "title": "Interpolated to 2040",
        }
    ]

    monkeypatch.setattr(
        InterpolateScenariosRunner,
        "run",
        lambda client, scenario_ids, end_years: ok_service_result(interpolated_data),
    )

    scenario_2030 = dummy_scenario(12345, end_year=2030)
    scenario_2050 = dummy_scenario(67890, end_year=2050)

    interpolated = Session.interpolate([scenario_2030, scenario_2050], 2040)

    assert len(interpolated) == 1
    assert interpolated[0].id == 88881
    assert interpolated[0].end_year == 2040
    assert len(interpolated[0].warnings) == 0


def test_interpolate_success_three_scenarios(
    monkeypatch, ok_service_result, dummy_scenario
):
    """Test batch interpolation with three scenarios and two target years"""
    interpolated_data = [
        {
            "id": 88881,
            "area_code": "nl",
            "end_year": 2040,
            "start_year": 2023,
            "title": "Interpolated to 2040",
        },
        {
            "id": 88882,
            "area_code": "nl",
            "end_year": 2060,
            "start_year": 2023,
            "title": "Interpolated to 2060",
        },
    ]

    monkeypatch.setattr(
        InterpolateScenariosRunner,
        "run",
        lambda client, scenario_ids, end_years: ok_service_result(interpolated_data),
    )

    scenario_2030 = dummy_scenario(12345, end_year=2030)
    scenario_2050 = dummy_scenario(45678, end_year=2050)
    scenario_2070 = dummy_scenario(67890, end_year=2070)

    interpolated = Session.interpolate(
        [scenario_2030, scenario_2050, scenario_2070], 2040, 2060
    )

    assert len(interpolated) == 2
    assert interpolated[0].id == 88881
    assert interpolated[0].end_year == 2040
    assert interpolated[1].id == 88882
    assert interpolated[1].end_year == 2060
    assert all(len(s.warnings) == 0 for s in interpolated)


def test_interpolate_with_warnings(monkeypatch, ok_service_result, dummy_scenario):
    """Test batch interpolation with warnings"""
    interpolated_data = [
        {"id": 88883, "area_code": "nl", "end_year": 2040, "start_year": 2023}
    ]
    warnings = ["Some inputs could not be interpolated"]

    monkeypatch.setattr(
        InterpolateScenariosRunner,
        "run",
        lambda client, scenario_ids, end_years: ok_service_result(
            interpolated_data, warnings
        ),
    )

    scenario_2030 = dummy_scenario(12345, end_year=2030)
    scenario_2050 = dummy_scenario(67890, end_year=2050)

    interpolated = Session.interpolate([scenario_2030, scenario_2050], 2040)

    assert len(interpolated) == 1
    assert interpolated[0].id == 88883
    base_warnings = interpolated[0].warnings.get_by_field("base")
    assert len(base_warnings) == 1
    assert base_warnings[0].message == warnings[0]


def test_interpolate_failure_too_few_scenarios(
    monkeypatch, fail_service_result, dummy_scenario
):
    """Test batch interpolation failure with too few scenarios"""
    monkeypatch.setattr(
        InterpolateScenariosRunner,
        "run",
        lambda client, scenario_ids, end_years: fail_service_result(
            ["must contain at least 2 scenarios"]
        ),
    )

    scenario = dummy_scenario(12345)

    with pytest.raises(ScenarioError, match="Interpolation failed"):
        Session.interpolate([scenario], 2040)


def test_interpolate_failure_validation_error(
    monkeypatch, fail_service_result, dummy_scenario
):
    """Test batch interpolation with validation error"""
    monkeypatch.setattr(
        InterpolateScenariosRunner,
        "run",
        lambda client, scenario_ids, end_years: fail_service_result(
            ["all scenarios must have the same area code"]
        ),
    )

    scenario_nl = dummy_scenario(12345, end_year=2030)
    scenario_de = dummy_scenario(67890, end_year=2050)

    with pytest.raises(ScenarioError, match="Interpolation failed"):
        Session.interpolate([scenario_nl, scenario_de], 2040)


def test_interpolate_with_custom_client(monkeypatch, ok_service_result, dummy_scenario):
    """Test batch interpolation with custom client"""
    from pyetm.clients import BaseClient

    interpolated_data = [
        {"id": 88884, "area_code": "nl", "end_year": 2040, "start_year": 2023}
    ]

    mock_client = BaseClient()

    calls = []

    def mock_run(client, scenario_ids, end_years):
        calls.append((client, scenario_ids, end_years))
        return ok_service_result(interpolated_data)

    monkeypatch.setattr(InterpolateScenariosRunner, "run", mock_run)

    scenario_2030 = dummy_scenario(12345, end_year=2030)
    scenario_2050 = dummy_scenario(67890, end_year=2050)

    interpolated = Session.interpolate(
        [scenario_2030, scenario_2050], 2040, client=mock_client
    )

    assert len(interpolated) == 1
    assert interpolated[0].id == 88884
    # Verify custom client was used
    assert len(calls) == 1
    assert calls[0][0] is mock_client


def test_interpolate_rejects_duplicate_end_years(dummy_scenario):
    """Test that interpolate raises ValueError when sessions have duplicate end_years"""
    scenario_2040_a = dummy_scenario(12345, end_year=2040)
    scenario_2040_b = dummy_scenario(67890, end_year=2040)
    scenario_2050 = dummy_scenario(11111, end_year=2050)

    with pytest.raises(ValueError, match="Sessions must have unique end_year values"):
        Session.interpolate([scenario_2040_a, scenario_2040_b, scenario_2050], 2045)


def test_interpolate_rejects_different_area_codes(dummy_scenario):
    """Test that interpolate raises ValueError when sessions have different area_codes"""
    scenario_nl = dummy_scenario(12345, area_code="nl", end_year=2030)
    scenario_de = dummy_scenario(67890, area_code="de", end_year=2050)

    with pytest.raises(ValueError, match="All sessions must have the same area_code"):
        Session.interpolate([scenario_nl, scenario_de], 2040)


# ------ Validation tests ------ #


def test_get_hourly_output_curves_invalid_carrier_type(scenario):
    """Test that get_hourly_output_curves raises ValueError for invalid carrier type"""
    with pytest.raises(ValueError) as exc_info:
        scenario.get_hourly_output_curves("invalid_carrier")

    assert "Invalid carrier type 'invalid_carrier'" in str(exc_info.value)
    assert "electricity" in str(exc_info.value)
    assert "heat" in str(exc_info.value)


def test_get_annual_exports_invalid_export_name(scenario):
    """Test that get_annual_exports raises ValueError for invalid export name"""
    with pytest.raises(ValueError) as exc_info:
        scenario.get_annual_exports("invalid_export")

    assert "Invalid export names: ['invalid_export']" in str(exc_info.value)
    assert "production_parameters" in str(exc_info.value)


def test_get_annual_exports_mixed_valid_and_invalid(scenario):
    """Test that get_annual_exports raises ValueError when some names are invalid"""
    with pytest.raises(ValueError) as exc_info:
        scenario.get_annual_exports(["energy_flow", "bad_export", "sankey"])

    error_message = str(exc_info.value)
    assert "Invalid export names" in error_message
    assert "bad_export" in error_message


def test_get_annual_exports_auto_converts_single_string(monkeypatch, scenario, ok_service_result):
    """Test that get_annual_exports auto-converts single string to list"""
    # Mock the retrieve_multiple method to capture the arguments
    calls = []

    def mock_retrieve_multiple(client, session, export_names):
        calls.append(export_names)
        return {}

    # Need to mock at the Session level since Scenario delegates to Session
    original_method = Session.get_annual_exports

    def mock_session_get_annual_exports(self, export_names):
        from pyetm.validators import validate_export_names
        validated_names = validate_export_names(export_names)
        calls.append(validated_names)
        return {}

    monkeypatch.setattr(Session, "get_annual_exports", mock_session_get_annual_exports)

    # Call with a single string
    scenario.get_annual_exports("energy_flow")

    # Verify it was converted to a list
    assert calls[-1] == ["energy_flow"]




def test_custom_curves_caching_behavior(monkeypatch):
    """Test that custom_curves property caches correctly"""
    from pyetm.services.scenario_runners.fetch_custom_curves import (
        FetchAllCustomCurveDataRunner,
    )
    from pyetm.services.service_result import ServiceResult

    scenario = Session(id=12345, area_code="nl", end_year=2050)

    call_count = {"count": 0}

    def mock_runner(client, scenario_obj, include_internal=True, include_unattached=False):
        call_count["count"] += 1
        return ServiceResult.ok([])

    monkeypatch.setattr(FetchAllCustomCurveDataRunner, "run", mock_runner)

    # First access of property should fetch
    result1 = scenario.custom_curves
    assert call_count["count"] == 1

    # Second access of property should use cache
    result2 = scenario.custom_curves
    assert call_count["count"] == 1  # Should not increment

    # Third access should still use cache
    result3 = scenario.custom_curves
    assert call_count["count"] == 1  # Should not increment


def test_get_custom_curves_bypasses_cache(monkeypatch):
    """Test that get_custom_curves() always fetches fresh data"""
    from pyetm.services.scenario_runners.fetch_custom_curves import (
        FetchAllCustomCurveDataRunner,
    )
    from pyetm.services.service_result import ServiceResult

    scenario = Session(id=12345, area_code="nl", end_year=2050)

    call_count = {"count": 0}

    def mock_runner(client, scenario_obj, include_internal=True, include_unattached=False):
        call_count["count"] += 1
        return ServiceResult.ok([])

    monkeypatch.setattr(FetchAllCustomCurveDataRunner, "run", mock_runner)

    # First call to get_custom_curves() should fetch
    result1 = scenario.get_custom_curves()
    assert call_count["count"] == 1

    # Second call should fetch again (no caching)
    result2 = scenario.get_custom_curves()
    assert call_count["count"] == 2

    # Call with include_internal=False should also fetch
    result3 = scenario.get_custom_curves(include_internal=False)
    assert call_count["count"] == 3

    # Call with include_unattached=True should also fetch
    result4 = scenario.get_custom_curves(include_unattached=True)
    assert call_count["count"] == 4


