from unittest.mock import Mock
import pytest
import pandas as pd
from pyetm.models.inputs import Inputs
from pyetm.models.custom_curves import CustomCurves
from pyetm.services.scenario_runners.fetch_custom_curves import (
    FetchAllCustomCurveDataRunner,
)
from pyetm.services.scenario_runners.fetch_inputs import FetchInputsRunner
from pyetm.services.scenario_runners.fetch_sortables import FetchSortablesRunner
from pyetm.services.scenario_runners.fetch_metadata import FetchMetadataRunner
from pyetm.models.sortables import Sortables
from pyetm.services.scenario_runners.update_metadata import UpdateMetadataRunner
from pyetm.services.scenario_runners.update_inputs import UpdateInputsRunner
from pyetm.services.scenario_runners.update_sortables import UpdateSortablesRunner
from pyetm.services.service_result import ServiceResult
from pyetm.models import scenario as scenario_module
from pyetm.models.scenario import SavedScenarioError, Scenario, ScenarioError
from pyetm.services.scenario_runners.base_runner import BaseRunner
from pyetm.services.scenario_runners.break_preset_link import BreakPresetLinkRunner
from pyetm.services.scenario_runners.create_saved_scenario import CreateSavedScenarioRunner
from pyetm.services.scenario_runners.destroy_saved_scenario import DestroySavedScenarioRunner
from pyetm.services.scenario_runners.discard_saved_scenario import DiscardSavedScenarioRunner
from pyetm.services.scenario_runners.fetch_saved_scenario import FetchSavedScenarioRunner
from pyetm.services.scenario_runners.save_snapshot import SaveSnapshotRunner
from pyetm.services.scenario_runners.saved_scenario_users_create import SavedScenarioUsersCreateRunner
from pyetm.services.scenario_runners.saved_scenario_users_destroy import SavedScenarioUsersDestroyRunner
from pyetm.services.scenario_runners.saved_scenario_users_index import SavedScenarioUsersIndexRunner
from pyetm.services.scenario_runners.saved_scenario_users_update import SavedScenarioUsersUpdateRunner
from pyetm.services.scenario_runners.update_saved_scenario import UpdateSavedScenarioRunner


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


def test_update_inputs_single_input(monkeypatch, scenario, ok_service_result, inputs_json):
    """Test updating a single input"""
    # Set up a cached inputs object first
    scenario._inputs = Inputs.from_json(inputs_json)
    # First input should be the float_input
    targeted_input = next(iter(scenario._inputs))
    new_value = 80.0

    updated_data = {"scenario": {"id": scenario.id, "user_values": {targeted_input.key: new_value}}}

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


def test_update_inputs_with_warnings(monkeypatch, scenario, inputs_json, ok_service_result):
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


def test_update_inputs_excludes_invalid(monkeypatch, scenario, inputs_json, ok_service_result):
    """Invalid inputs are excluded from the upload; valid inputs still go through"""
    scenario._inputs = Inputs.from_json(inputs_json)

    sent_payloads = []

    def mock_runner_run(client, scen, inputs):
        sent_payloads.append(inputs)
        return ok_service_result({"scenario": {"id": scen.id}})

    monkeypatch.setattr(UpdateInputsRunner, "run", mock_runner_run)

    scenario.update_user_values(
        {"investment_costs_co2_ccs": 42.5, "invalid_input": "bad_value"}
    )

    # Only the valid input is sent to the API
    assert sent_payloads == [{"investment_costs_co2_ccs": 42.5}]
    # The valid input is still applied locally
    assert scenario.inputs["investment_costs_co2_ccs"].user == 42.5
    # The user is made aware the other input was excluded
    assert scenario.inputs.warnings.has_warnings("invalid_input")


def test_update_inputs_all_invalid_skips_api_call(monkeypatch, scenario, inputs_json):
    """When every input is invalid, no API call is made and no exception is raised"""
    scenario._inputs = Inputs.from_json(inputs_json)

    def mock_runner_run(client, scen, inputs):
        raise AssertionError("API should not be called when there are no valid updates")

    monkeypatch.setattr(UpdateInputsRunner, "run", mock_runner_run)

    scenario.update_user_values({"invalid_input": "bad_value"})

    assert scenario.inputs.warnings.has_warnings("invalid_input")


def test_update_inputs_failure(monkeypatch, scenario, inputs_json, fail_service_result):
    """A genuine API/server-side rejection of a valid payload still raises ScenarioError"""
    scenario._inputs = Inputs.from_json(inputs_json)

    monkeypatch.setattr(
        UpdateInputsRunner,
        "run",
        lambda client, scen, inputs: fail_service_result(["422: Invalid input value"]),
    )

    with pytest.raises(ScenarioError, match="Could not update user values"):
        scenario.update_user_values({"investment_costs_co2_ccs": 42.5})


def test_update_inputs_empty_dict(monkeypatch, scenario, ok_service_result, inputs_json):
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
        pyetm.services.scenario_runners.update_inputs.UpdateInputsRunner.run = original_run


# ------ sortables ------ #


@pytest.fixture
def patch_sortables_from_json(monkeypatch):
    dummy = object()
    monkeypatch.setattr(Sortables, "from_json", staticmethod(lambda data: dummy))
    return dummy


def test_sortables_success(monkeypatch, patch_sortables_from_json, scenario, ok_service_result):
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

    monkeypatch.setattr(UpdateSortablesRunner, "run", lambda *args, **kwargs: ok_service_result({}))

    scenario.update_sortables(updates)

    mock_sortables.is_valid_update.assert_called_once_with(updates)
    mock_sortables.update.assert_called_once_with(updates)


def test_update_sortables_validation_error(monkeypatch, scenario):
    from pyetm.models.warnings import WarningCollector

    updates = {"nonexistent": [1, 2, 3]}

    mock_sortables = Mock()
    error_collector = WarningCollector.with_warning("nonexistent", "Sortable does not exist")
    mock_sortables.is_valid_update.return_value = {"nonexistent": error_collector}
    mock_sortables.update = Mock()
    scenario._sortables = mock_sortables

    def mock_runner_run(*args, **kwargs):
        raise AssertionError("API should not be called when there are no valid updates")

    monkeypatch.setattr(UpdateSortablesRunner, "run", mock_runner_run)

    # Invalid sortable is excluded; no exception is raised
    scenario.update_sortables(updates)

    # Local cache is still synced with the full (unfiltered) dict, which warns
    # about the excluded sortable without applying it
    mock_sortables.update.assert_called_once_with(updates)


def test_update_sortables_excludes_invalid(monkeypatch, scenario, ok_service_result):
    from pyetm.models.warnings import WarningCollector

    updates = {"forecast_storage": [1, 2, 3], "nonexistent": [4, 5, 6]}

    mock_sortables = Mock()
    error_collector = WarningCollector.with_warning("nonexistent", "Sortable does not exist")
    mock_sortables.is_valid_update.return_value = {"nonexistent": error_collector}
    mock_sortables.update = Mock()
    scenario._sortables = mock_sortables

    calls = []

    def mock_runner_run(client, scen, name, order, subtype=None):
        calls.append((name, order))
        return ok_service_result({})

    monkeypatch.setattr(UpdateSortablesRunner, "run", mock_runner_run)

    scenario.update_sortables(updates)

    # Only the valid sortable is sent to the API
    assert calls == [("forecast_storage", [1, 2, 3])]
    mock_sortables.update.assert_called_once_with(updates)


def test_remove_sortables(monkeypatch, scenario, ok_service_result):
    sortable_names = ["forecast_storage", "hydrogen_supply"]

    mock_sortables = Mock()
    mock_sortables.update = Mock()
    scenario._sortables = mock_sortables

    monkeypatch.setattr(UpdateSortablesRunner, "run", lambda *args, **kwargs: ok_service_result({}))

    scenario.remove_sortables(sortable_names)

    expected_updates = {"forecast_storage": [], "hydrogen_supply": []}
    mock_sortables.update.assert_called_once_with(expected_updates)


# ------ custom_curves ------ #


@pytest.fixture(autouse=True)
def patch_custom_curves_from_json(monkeypatch):
    dummy = CustomCurves(curves=[])
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


def test_to_dataframe_merges_myetm_and_session_fields(monkeypatch, ok_service_result):
    scenario = Scenario(id=7, scenario_id=70, title="My title", area_code="nl2019", end_year=2050)
    monkeypatch.setattr(
        FetchMetadataRunner,
        "run",
        lambda client, scen: ok_service_result({"start_year": 2019, "metadata": {"foo": "bar"}}),
    )

    column = scenario.to_dataframe()["7"]

    assert column["title"] == "My title"
    assert column["saved_scenario_id"] == 7
    assert column["session_id"] == 70
    assert column["start_year"] == 2019
    assert column["foo"] == "bar"


def test_to_dataframe_keeps_the_title_beside_the_short_name(monkeypatch, ok_service_result):
    scenario = Scenario(id=7, area_code="nl2019", end_year=2050, title="My title")
    scenario.set_short_name("short")
    monkeypatch.setattr(FetchMetadataRunner, "run", lambda client, scen: ok_service_result({}))

    column = scenario.to_dataframe()["7"]

    assert column["title"] == "My title"
    assert column["short_name"] == "short"


# ------ Warnings tests ------ #


def test_scenario_warning_system_integration(scenario):
    """Test that the scenario properly integrates with the new warning system"""
    # Set bulk context to prevent raising on error severity
    scenario.set_bulk_context(True)

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


def test_scenario_update_custom_curves_validation_error(monkeypatch):
    """Invalid curves are excluded from the upload; no exception is raised"""
    from pyetm.models.custom_curves import CustomCurve, CustomCurves
    from pyetm.models.warnings import WarningCollector
    from pyetm.services.scenario_runners.update_custom_curves import (
        UpdateCustomCurvesRunner,
    )

    scenario = Scenario(id=12345, area_code="nl", end_year=2050)
    scenario._custom_curves = CustomCurves(curves=[])

    # Create custom curves
    curve = CustomCurve(key="invalid_curve", type="profile")
    custom_curves = CustomCurves(curves=[curve])

    # Mock validate_for_upload to return validation errors
    def mock_validate():
        warning_collector = WarningCollector()
        warning_collector.add("invalid_curve", "Curve contains non-numeric values")
        return {"invalid_curve": warning_collector}

    custom_curves.validate_for_upload = mock_validate

    def mock_runner_run(*args, **kwargs):
        raise AssertionError("API should not be called when there are no valid curves")

    monkeypatch.setattr(UpdateCustomCurvesRunner, "run", mock_runner_run)

    # Should not raise; the invalid curve is excluded from the upload
    scenario.update_custom_curves(custom_curves)

    warning_messages = [w.message for w in scenario.warnings.get_by_field("custom_curves")]
    assert any("invalid_curve" in m for m in warning_messages)
    assert any("Curve contains non-numeric values" in m for m in warning_messages)
    # The excluded curve is not merged into the local collection
    assert len(scenario.custom_curves.curves) == 0


def test_scenario_update_custom_curves_excludes_invalid(monkeypatch, ok_service_result):
    """A mix of valid and invalid curves: only the valid one is uploaded and merged"""
    from pyetm.models.custom_curves import CustomCurve, CustomCurves
    from pyetm.models.warnings import WarningCollector
    from pyetm.services.scenario_runners.update_custom_curves import (
        UpdateCustomCurvesRunner,
    )

    scenario = Scenario(id=12345, area_code="nl", end_year=2050)
    scenario._custom_curves = CustomCurves(curves=[])

    valid_curve = CustomCurve(key="valid_curve", type="profile")
    invalid_curve = CustomCurve(key="invalid_curve", type="profile")
    custom_curves = CustomCurves(curves=[valid_curve, invalid_curve])

    def mock_validate():
        warning_collector = WarningCollector()
        warning_collector.add("invalid_curve", "Curve has no data available")
        return {"invalid_curve": warning_collector}

    custom_curves.validate_for_upload = mock_validate

    uploaded_keys = []

    def mock_runner_run(client, scen, curves):
        uploaded_keys.extend(curve.key for curve in curves.curves)
        return ok_service_result({"uploaded_curves": uploaded_keys})

    monkeypatch.setattr(UpdateCustomCurvesRunner, "run", mock_runner_run)

    scenario.update_custom_curves(custom_curves)

    # Only the valid curve is sent to the API
    assert uploaded_keys == ["valid_curve"]
    # Only the valid curve is merged locally
    assert [c.key for c in scenario.custom_curves.curves] == ["valid_curve"]
    warning_messages = [w.message for w in scenario.warnings.get_by_field("custom_curves")]
    assert any("invalid_curve" in m for m in warning_messages)


def test_scenario_update_custom_curves_runner_failure(monkeypatch, fail_service_result):
    """Test custom curves update with runner failure"""
    from pyetm.models.custom_curves import CustomCurve, CustomCurves
    from pyetm.services.scenario_runners.update_custom_curves import (
        UpdateCustomCurvesRunner,
    )

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
    from pyetm.services.scenario_runners.update_custom_curves import (
        UpdateCustomCurvesRunner,
    )
    from pathlib import Path

    scenario = Scenario(id=12345, area_code="nl", end_year=2050)

    # Set up scenario with existing curve
    existing_curve = CustomCurve(
        key="existing_curve", type="profile", file_path=Path("/old/path.csv")
    )
    scenario._custom_curves = CustomCurves(curves=[existing_curve])

    # Create new curves with same key but different file path
    new_curve = CustomCurve(key="existing_curve", type="profile", file_path=Path("/new/path.csv"))
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

    scenario = Scenario(id=12345, area_code="nl", end_year=2050)

    # Set up scenario with one existing curve
    existing_curve = CustomCurve(
        key="existing_curve", type="profile", file_path=Path("/old/path.csv")
    )
    scenario._custom_curves = CustomCurves(curves=[existing_curve])

    # Create new curve with different key
    new_curve = CustomCurve(key="new_curve", type="availability", file_path=Path("/new/path.csv"))
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


def test_scenario_update_custom_curves_multiple_validation_errors(monkeypatch):
    """All curves invalid: none are uploaded, no exception is raised, warnings cover both"""
    from pyetm.models.custom_curves import CustomCurve, CustomCurves
    from pyetm.models.warnings import WarningCollector
    from pyetm.services.scenario_runners.update_custom_curves import (
        UpdateCustomCurvesRunner,
    )

    scenario = Scenario(id=12345, area_code="nl", end_year=2050)
    scenario._custom_curves = CustomCurves(curves=[])

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

    def mock_runner_run(*args, **kwargs):
        raise AssertionError("API should not be called when there are no valid curves")

    monkeypatch.setattr(UpdateCustomCurvesRunner, "run", mock_runner_run)

    # Should not raise; both curves are excluded from the upload
    scenario.update_custom_curves(custom_curves)

    warning_messages = " ".join(
        w.message for w in scenario.warnings.get_by_field("custom_curves")
    )
    assert "curve1" in warning_messages
    assert "curve2" in warning_messages
    assert "Wrong length" in warning_messages
    assert "Non-numeric values" in warning_messages
    assert "No data available" in warning_messages
    assert len(scenario.custom_curves.curves) == 0


def test_scenario_remove_custom_curves_success(monkeypatch, ok_service_result):
    """Test successful custom curves removal"""
    from pyetm.models.custom_curves import CustomCurve, CustomCurves
    from pyetm.services.scenario_runners.delete_custom_curves import (
        DeleteCustomCurvesRunner,
    )
    from pathlib import Path
    from unittest.mock import Mock

    scenario = Scenario(id=12345, area_code="nl", end_year=2050)

    # Set up scenario with existing curves
    curve1 = CustomCurve(key="curve_to_remove", type="profile", file_path=Path("/tmp/curve1.csv"))
    curve2 = CustomCurve(key="curve_to_keep", type="availability", file_path=Path("/tmp/curve2.csv"))
    scenario._custom_curves = CustomCurves(curves=[curve1, curve2])

    # Mock curve.remove() to avoid actual file operations
    curve1.remove = Mock()
    curve2.remove = Mock()

    # Mock DeleteCustomCurvesRunner to succeed
    def mock_runner(client, scenario, curve_keys):
        return ok_service_result(
            {
                "deleted_curves": ["curve_to_remove"],
                "total_curves": 1,
                "successful_deletions": 1,
            }
        )

    monkeypatch.setattr(DeleteCustomCurvesRunner, "run", mock_runner)

    # Remove one curve
    scenario.remove_custom_curves(["curve_to_remove"])

    # Verify curve was removed from local collection
    assert len(scenario._custom_curves.curves) == 1
    assert scenario._custom_curves.curves[0].key == "curve_to_keep"

    # Verify cache file was cleared
    curve1.remove.assert_called_once()
    curve2.remove.assert_not_called()


def test_scenario_remove_custom_curves_multiple(monkeypatch, ok_service_result):
    """Test removing multiple custom curves"""
    from pyetm.models.custom_curves import CustomCurve, CustomCurves
    from pyetm.services.scenario_runners.delete_custom_curves import (
        DeleteCustomCurvesRunner,
    )
    from pathlib import Path
    from unittest.mock import Mock

    scenario = Scenario(id=12345, area_code="nl", end_year=2050)

    # Set up scenario with three curves
    curves = [
        CustomCurve(key="curve1", type="profile", file_path=Path("/tmp/curve1.csv")),
        CustomCurve(key="curve2", type="availability", file_path=Path("/tmp/curve2.csv")),
        CustomCurve(key="curve3", type="profile", file_path=Path("/tmp/curve3.csv")),
    ]
    for curve in curves:
        curve.remove = Mock()

    scenario._custom_curves = CustomCurves(curves=curves)

    # Mock DeleteCustomCurvesRunner to succeed
    def mock_runner(client, scenario, curve_keys):
        return ok_service_result(
            {
                "deleted_curves": ["curve1", "curve3"],
                "total_curves": 2,
                "successful_deletions": 2,
            }
        )

    monkeypatch.setattr(DeleteCustomCurvesRunner, "run", mock_runner)

    # Remove two curves
    scenario.remove_custom_curves(["curve1", "curve3"])

    # Verify only curve2 remains
    assert len(scenario._custom_curves.curves) == 1
    assert scenario._custom_curves.curves[0].key == "curve2"

    # Verify cache files were cleared for removed curves
    curves[0].remove.assert_called_once()
    curves[1].remove.assert_not_called()
    curves[2].remove.assert_called_once()


def test_scenario_remove_custom_curves_empty_list(monkeypatch):
    """Test removing custom curves with empty list"""
    from pyetm.models.custom_curves import CustomCurve, CustomCurves
    from pathlib import Path

    scenario = Scenario(id=12345, area_code="nl", end_year=2050)

    # Set up scenario with curves
    curve = CustomCurve(key="existing_curve", type="profile", file_path=Path("/tmp/curve.csv"))
    scenario._custom_curves = CustomCurves(curves=[curve])

    # Remove empty list (should be no-op)
    scenario.remove_custom_curves([])

    # Verify curve is still there
    assert len(scenario._custom_curves.curves) == 1
    assert scenario._custom_curves.curves[0].key == "existing_curve"


def test_scenario_remove_custom_curves_with_set(monkeypatch, ok_service_result):
    """Test removing custom curves with set instead of list"""
    from pyetm.models.custom_curves import CustomCurve, CustomCurves
    from pyetm.services.scenario_runners.delete_custom_curves import (
        DeleteCustomCurvesRunner,
    )
    from pathlib import Path
    from unittest.mock import Mock

    scenario = Scenario(id=12345, area_code="nl", end_year=2050)

    # Set up scenario with curves
    curve1 = CustomCurve(key="curve1", type="profile", file_path=Path("/tmp/curve1.csv"))
    curve2 = CustomCurve(key="curve2", type="availability", file_path=Path("/tmp/curve2.csv"))
    curve1.remove = Mock()
    curve2.remove = Mock()

    scenario._custom_curves = CustomCurves(curves=[curve1, curve2])

    # Mock DeleteCustomCurvesRunner to succeed
    def mock_runner(client, scenario, curve_keys):
        return ok_service_result(
            {
                "deleted_curves": list(curve_keys),
                "total_curves": len(curve_keys),
                "successful_deletions": len(curve_keys),
            }
        )

    monkeypatch.setattr(DeleteCustomCurvesRunner, "run", mock_runner)

    # Remove using a set
    scenario.remove_custom_curves({"curve1"})

    # Verify only curve2 remains
    assert len(scenario._custom_curves.curves) == 1
    assert scenario._custom_curves.curves[0].key == "curve2"


def test_scenario_remove_custom_curves_runner_failure(monkeypatch, fail_service_result):
    """Test custom curves removal with runner failure"""
    from pyetm.models.custom_curves import CustomCurve, CustomCurves
    from pyetm.services.scenario_runners.delete_custom_curves import (
        DeleteCustomCurvesRunner,
    )
    from pathlib import Path
    from unittest.mock import Mock

    scenario = Scenario(id=12345, area_code="nl", end_year=2050)

    # Set up scenario with curves
    curve = CustomCurve(key="test_curve", type="profile", file_path=Path("/tmp/curve.csv"))
    curve.remove = Mock()
    scenario._custom_curves = CustomCurves(curves=[curve])

    # Mock DeleteCustomCurvesRunner to fail
    def mock_runner(client, scenario, curve_keys):
        return fail_service_result(["HTTP 500: Internal server error"])

    monkeypatch.setattr(DeleteCustomCurvesRunner, "run", mock_runner)

    # Should raise ScenarioError due to runner failure
    with pytest.raises(ScenarioError) as exc_info:
        scenario.remove_custom_curves(["test_curve"])

    assert "Could not remove custom curves" in str(exc_info.value)
    assert "HTTP 500: Internal server error" in str(exc_info.value)

    # Verify curve was NOT removed from local collection
    assert len(scenario._custom_curves.curves) == 1
    assert scenario._custom_curves.curves[0].key == "test_curve"



# ------ Validation tests ------ #


def test_get_hourly_curve_by_carrier_alias(scenario):
    """Test that get_hourly_curve works with carrier type aliases"""
    # Mock the hourly_output_curves collection
    scenario.hourly_output_curves.get_curve = Mock(return_value=pd.DataFrame())

    # Test carrier aliases
    scenario.get_hourly_curve("electricity")
    scenario.hourly_output_curves.get_curve.assert_called_with("electricity", scenario)

    scenario.get_hourly_curve("heat")
    scenario.hourly_output_curves.get_curve.assert_called_with("heat", scenario)


def test_get_hourly_curves_multiple(scenario):
    """Test that get_hourly_curves works with multiple identifiers"""
    # Mock the hourly_output_curves collection
    scenario.hourly_output_curves.get_curves = Mock(return_value={})

    # Test with list of identifiers
    scenario.get_hourly_curves(["electricity", "heat", "electricity_profiles"])
    scenario.hourly_output_curves.get_curves.assert_called_with(
        ["electricity", "heat", "electricity_profiles"], scenario
    )


def test_get_annual_exports_invalid_export_name(scenario):
    """Test that get_annual_exports raises ValueError for invalid export name"""
    with pytest.raises(ValueError) as exc_info:
        scenario.get_annual_exports("invalid_export")

    assert "Invalid export names: ['invalid_export']" in str(exc_info.value)
    assert "costs_parameters" in str(exc_info.value)


def test_get_annual_exports_mixed_valid_and_invalid(scenario):
    """Test that get_annual_exports raises ValueError when some names are invalid"""
    with pytest.raises(ValueError) as exc_info:
        scenario.get_annual_exports(["energy_flow", "bad_export", "sankey"])

    error_message = str(exc_info.value)
    assert "Invalid export names" in error_message
    assert "bad_export" in error_message




# ============================================================================
# Cache Clearing Tests
# ============================================================================


def test_clear_hourly_curves_cache(scenario):
    """Test clearing hourly output curves cache."""
    # Create mock collection - scenario fixture is already a Scenario
    mock_collection = Mock()
    mock_collection.clear_cache = Mock(return_value=5)
    scenario._hourly_output_curves = mock_collection

    removed_count = scenario.clear_hourly_curves_cache()

    assert removed_count == 5
    mock_collection.clear_cache.assert_called_once()


def test_clear_custom_curves_cache(scenario):
    """Test clearing custom curves cache."""
    # Create mock collection - scenario fixture is already a Scenario
    mock_collection = Mock()
    mock_collection.clear_cache = Mock(return_value=3)
    scenario._custom_curves = mock_collection

    removed_count = scenario.clear_custom_curves_cache()

    assert removed_count == 3
    mock_collection.clear_cache.assert_called_once()


def test_clear_all_curve_caches(scenario):
    """Test clearing all curve caches at once."""
    # Create mock collections - scenario fixture is already a Scenario
    mock_hourly = Mock()
    mock_hourly.clear_cache = Mock(return_value=5)
    scenario._hourly_output_curves = mock_hourly

    mock_custom = Mock()
    mock_custom.clear_cache = Mock(return_value=3)
    scenario._custom_curves = mock_custom

    hourly, custom = scenario.clear_all_curve_caches()

    assert hourly == 5
    assert custom == 3
    mock_hourly.clear_cache.assert_called_once()
    mock_custom.clear_cache.assert_called_once()




def test_get_annual_exports_accepts_a_single_name(monkeypatch, scenario):
    calls = []
    monkeypatch.setattr(
        type(scenario.annual_exports),
        "retrieve_multiple",
        lambda self, client, scen, names: calls.append(names) or {},
    )

    scenario.get_annual_exports("energy_flow")

    assert calls == [["energy_flow"]]


# ------ Creating, copying, interpolating and saving through MyETM ------ #

def myetm_record(**overrides):
    record = {
        "id": 789,
        "scenario_id": 123,
        "title": "New",
        "area_code": "nl2023",
        "end_year": 2050,
        "version": "latest",
        "private": False,
    }
    record.update(overrides)
    return record


@pytest.fixture
def created(monkeypatch):
    """Records what each create sent, and answers with a MyETM record."""
    sent = []

    def create(client, params):
        sent.append(params)
        return ServiceResult.ok(myetm_record(id=789 + len(sent) - 1, title=params["title"]))

    monkeypatch.setattr(CreateSavedScenarioRunner, "run", create)
    return sent


@pytest.fixture(autouse=True)
def forget_warned_versions():
    scenario_module._versions_warned.clear()
    yield
    scenario_module._versions_warned.clear()


def test_new_creates_on_etm_version_and_drops_unset_members(created, mock_client):
    scenario = Scenario.new(title="New", area_code="nl2023", end_year=2050, client=mock_client)

    assert created == [{"title": "New", "area_code": "nl2023", "end_year": 2050, "version": "latest"}]
    assert scenario.id == 789
    assert scenario.client is mock_client


def test_new_keeps_runner_warnings(monkeypatch, mock_client):
    monkeypatch.setattr(
        CreateSavedScenarioRunner,
        "run",
        lambda client, params: ServiceResult.ok(myetm_record(), ["Ignoring invalid field"]),
    )

    scenario = Scenario.new(title="New", area_code="nl2023", end_year=2050, client=mock_client)

    assert [w.message for w in scenario.warnings.get_by_field("base")] == ["Ignoring invalid field"]


def test_new_failure_raises_and_the_old_name_still_catches(monkeypatch, mock_client):
    monkeypatch.setattr(
        CreateSavedScenarioRunner, "run", lambda client, params: ServiceResult.fail(["422: bad"])
    )

    with pytest.raises(SavedScenarioError, match="Could not create saved scenario"):
        Scenario.new(title="New", area_code="nl2023", end_year=2050, client=mock_client)
    assert SavedScenarioError is ScenarioError


def test_new_sends_user_values_to_the_new_scenario(monkeypatch, created, mock_client):
    batches = []
    monkeypatch.setattr(
        BaseRunner,
        "_make_batch_requests",
        classmethod(lambda cls, client, requests: batches.append(requests) or [ServiceResult.ok({})]),
    )

    Scenario.new(title="New", area_code="nl", end_year=2050, client=mock_client, user_values={"a": 1})

    (request,) = batches[0]
    assert request["path"] == "/saved_scenarios/789/"
    assert request["payload"] == {"scenario": {"user_values": {"a": 1}}}


def test_new_turns_a_failed_data_upload_into_a_warning(monkeypatch, created, mock_client):
    monkeypatch.setattr(
        BaseRunner,
        "_make_batch_requests",
        classmethod(lambda cls, client, requests: [ServiceResult.fail(["422: no"])]),
    )

    scenario = Scenario.new(title="New", area_code="nl", end_year=2050, client=mock_client, user_values={"a": 1})

    assert any("422: no" in w.message for w in scenario.warnings.get_by_field("base"))


def test_copy_with_preset_creates_from_this_scenario(created, saved_scenario):
    saved_scenario.version = "latest"

    copied = saved_scenario.copy_with_preset(private=True)

    assert created == [
        {"title": "Copy of My Saved Scenario", "source": 456, "version": "latest", "private": True}
    ]
    assert copied.id == 789


def test_copy_breaks_the_preset_link(monkeypatch, created, saved_scenario):
    broken = []
    monkeypatch.setattr(
        BreakPresetLinkRunner, "run", lambda client, scen: broken.append(scen.id) or ServiceResult.ok({})
    )

    copied = saved_scenario.copy(title="Independent")

    assert created[0]["title"] == "Independent"
    assert broken == [copied.id]


def test_copy_raises_when_the_preset_link_stays(monkeypatch, created, saved_scenario):
    monkeypatch.setattr(BreakPresetLinkRunner, "run", lambda client, scen: ServiceResult.fail(["502"]))

    with pytest.raises(ScenarioError, match="could not break its preset link"):
        saved_scenario.copy()


def test_interpolate_creates_one_scenario_per_year_from_the_source(created, saved_scenario):
    saved_scenario.version = "latest"

    results = Scenario.interpolate(saved_scenario, 2030, 2040)

    assert [r["end_year"] for r in created] == [2030, 2040]
    assert {r["source"] for r in created} == {456}
    assert created[0]["title"] == "My Saved Scenario interpolated to 2030"
    assert len(results) == 2


def test_interpolate_accepts_a_list_of_one_and_custom_titles(created, saved_scenario):
    Scenario.interpolate([saved_scenario], 2030, titles=["Mid"])

    assert created[0]["title"] == "Mid"


def test_interpolate_refuses_several_sources(saved_scenario):
    with pytest.raises(ValueError, match="several scenarios"):
        Scenario.interpolate([saved_scenario, saved_scenario], 2030)


def test_interpolate_refuses_mismatched_titles(saved_scenario):
    with pytest.raises(ValueError, match="Length of titles"):
        Scenario.interpolate(saved_scenario, 2030, 2040, titles=["one"])


def test_save_refreshes_the_current_session(monkeypatch, saved_scenario):
    monkeypatch.setattr(
        SaveSnapshotRunner, "run", lambda client, ssid: ServiceResult.ok(myetm_record(id=ssid, scenario_id=999))
    )

    saved_scenario.save()

    assert saved_scenario.scenario_id == 999


def test_save_failure_raises(monkeypatch, saved_scenario):
    monkeypatch.setattr(SaveSnapshotRunner, "run", lambda client, ssid: ServiceResult.fail(["403: forbidden"]))

    with pytest.raises(ScenarioError, match="Could not save scenario 456"):
        saved_scenario.save()


# ------ The MyETM record ------ #


def test_load_keeps_the_client(monkeypatch, mock_client):
    monkeypatch.setattr(
        FetchSavedScenarioRunner, "run", lambda client, ssid: ServiceResult.ok(myetm_record(id=ssid))
    )

    scenario = Scenario.load(5, client=mock_client)

    assert scenario.id == 5
    assert scenario.client is mock_client


def test_load_failure_names_the_scenario(monkeypatch, mock_client):
    monkeypatch.setattr(
        FetchSavedScenarioRunner, "run", lambda client, ssid: ServiceResult.fail(["404: scenario_not_found"])
    )

    with pytest.raises(ScenarioError, match="Could not load saved scenario 5: .*404"):
        Scenario.load(5, client=mock_client)


def test_another_version_warns_once(monkeypatch, mock_client, caplog):
    monkeypatch.setattr(
        FetchSavedScenarioRunner,
        "run",
        lambda client, ssid: ServiceResult.ok(myetm_record(id=ssid, version="2025-01")),
    )

    with caplog.at_level("WARNING", logger="pyetm.models.scenario"):
        Scenario.load(1, client=mock_client)
        Scenario.load(2, client=mock_client)

    assert sum("Version 2025-01" in r.message for r in caplog.records) == 1


def test_the_targeted_version_does_not_warn(monkeypatch, mock_client, caplog):
    monkeypatch.setattr(
        FetchSavedScenarioRunner, "run", lambda client, ssid: ServiceResult.ok(myetm_record(id=ssid))
    )

    with caplog.at_level("WARNING", logger="pyetm.models.scenario"):
        Scenario.load(1, client=mock_client)

    assert not [r for r in caplog.records if "Version" in r.message]


def test_update_refreshes_from_the_response(monkeypatch, saved_scenario):
    sent = []

    def update(client, ssid, data):
        sent.append(data)
        return ServiceResult.ok(myetm_record(id=ssid, title="Renamed"), ["Ignoring invalid field"])

    monkeypatch.setattr(UpdateSavedScenarioRunner, "run", update)

    saved_scenario.update(title="Renamed")

    assert sent == [{"title": "Renamed"}]
    assert saved_scenario.title == "Renamed"
    assert [w.message for w in saved_scenario.warnings.get_by_field("update")] == ["Ignoring invalid field"]


def test_update_failure_raises(monkeypatch, saved_scenario):
    monkeypatch.setattr(UpdateSavedScenarioRunner, "run", lambda client, ssid, data: ServiceResult.fail(["422"]))

    with pytest.raises(ScenarioError, match="Could not update saved scenario"):
        saved_scenario.update(title="")


def test_discard_records_when(monkeypatch, saved_scenario):
    monkeypatch.setattr(
        DiscardSavedScenarioRunner,
        "run",
        lambda client, ssid: ServiceResult.ok(myetm_record(id=ssid, discarded_at="2026-11-20T10:00:00Z")),
    )

    saved_scenario.discard()

    assert saved_scenario.discarded_at is not None


def test_delete_failure_raises(monkeypatch, saved_scenario):
    monkeypatch.setattr(DestroySavedScenarioRunner, "run", lambda client, ssid: ServiceResult.fail(["403"]))

    with pytest.raises(ScenarioError, match="Could not permanently delete"):
        saved_scenario.delete()


# ------ The current Session's metadata ------ #


def test_session_metadata_is_fetched_once(monkeypatch, saved_scenario):
    calls = []

    def fetch(client, scen):
        calls.append(scen.id)
        return ServiceResult.ok(
            {"start_year": 2019, "url": "https://engine/1", "preset_scenario_id": 5, "keep_compatible": True}
        )

    monkeypatch.setattr(FetchMetadataRunner, "run", fetch)

    assert (saved_scenario.start_year, saved_scenario.url, saved_scenario.template_id) == (
        2019,
        "https://engine/1",
        5,
    )
    assert saved_scenario.keep_compatible is True
    assert calls == [456]


def test_session_metadata_failure_raises(monkeypatch, saved_scenario):
    monkeypatch.setattr(FetchMetadataRunner, "run", lambda client, scen: ServiceResult.fail(["502"]))

    with pytest.raises(ScenarioError, match="from the engine"):
        saved_scenario.start_year


def test_update_metadata_replaces_the_cached_session(monkeypatch, saved_scenario):
    monkeypatch.setattr(
        UpdateMetadataRunner,
        "run",
        lambda client, scen, metadata: ServiceResult.ok({"scenario": {"keep_compatible": True}}),
    )
    monkeypatch.setattr(FetchMetadataRunner, "run", Mock(side_effect=AssertionError("fetched")))

    saved_scenario.update_metadata(keep_compatible=True)

    assert saved_scenario.keep_compatible is True


def test_identifier_prefers_title_then_short_name_then_id():
    scenario = Scenario(id=3, title="Title")
    assert scenario.identifier() == "Title"

    scenario = Scenario(id=3)
    scenario.set_short_name("short")
    assert scenario.identifier() == "short"

    assert Scenario(id=3).identifier() == 3


# ------ Users ------ #


def test_list_users_shortens_roles(monkeypatch, saved_scenario):
    monkeypatch.setattr(
        SavedScenarioUsersIndexRunner,
        "run",
        lambda client, ssid: ServiceResult.ok(
            [{"email": "a@example.com", "role": "scenario_owner", "pending": False}]
        ),
    )

    assert saved_scenario.list_users() == [
        {"user_email": "a@example.com", "role": "owner", "pending": False}
    ]


@pytest.fixture
def user_calls(monkeypatch):
    calls = []
    for name, runner in (
        ("add", SavedScenarioUsersCreateRunner),
        ("update", SavedScenarioUsersUpdateRunner),
        ("remove", SavedScenarioUsersDestroyRunner),
    ):
        monkeypatch.setattr(
            runner,
            "run",
            lambda client, ssid, users, name=name: calls.append((name, users)) or ServiceResult.ok([]),
        )
    monkeypatch.setattr(
        SavedScenarioUsersIndexRunner,
        "run",
        lambda client, ssid: ServiceResult.ok([{"email": "known@example.com", "role": "scenario_viewer"}]),
    )
    return calls


def test_update_users_adds_a_new_user(user_calls, saved_scenario):
    saved_scenario.update_users("new@example.com", "viewer")

    assert user_calls == [("add", [{"email": "new@example.com", "role": "scenario_viewer"}])]


def test_update_users_changes_a_known_users_role(user_calls, saved_scenario):
    saved_scenario.update_users("known@example.com", "scenario_collaborator")

    assert user_calls == [("update", [{"email": "known@example.com", "role": "scenario_collaborator"}])]


def test_update_users_removes(user_calls, saved_scenario):
    saved_scenario.update_users("known@example.com", "remove")

    assert user_calls == [("remove", [{"email": "known@example.com"}])]


def test_update_users_failure_raises(monkeypatch, user_calls, saved_scenario):
    monkeypatch.setattr(
        SavedScenarioUsersCreateRunner, "run", lambda client, ssid, users: ServiceResult.fail(["validation_failed"])
    )

    with pytest.raises(ScenarioError, match="Could not add user"):
        saved_scenario.update_users("new@example.com", "owner")


def test_pending_users_wait_until_applied(user_calls, saved_scenario):
    saved_scenario.update_users("new@example.com", "owner", skip_upload=True)
    assert user_calls == []

    assert saved_scenario.apply_pending_users() == 1
    assert user_calls == [("add", [{"email": "new@example.com", "role": "scenario_owner"}])]
    assert saved_scenario.apply_pending_users() == 0


@pytest.mark.parametrize(
    "role, expected",
    [("owner", "scenario_owner"), ("Viewer", "scenario_viewer"), ("scenario_collaborator", "scenario_collaborator"), ("remove", "remove")],
)
def test_normalize_role(role, expected):
    assert Scenario._normalize_role(role) == expected


def test_normalize_role_refuses_unknown_roles():
    with pytest.raises(ValueError, match="Invalid role: admin"):
        Scenario._normalize_role("admin")
