"""
Tests for Scenarios.create_many() with user_values, custom_curves, sortables and sources.
"""

import pytest
import pandas as pd
from unittest.mock import Mock, patch
from pyetm.models.scenarios import Scenarios, ScenarioCreationParams
from pyetm.models.scenario import Scenario, ScenarioError


def saved(id_val=10):
    scenario = Mock(spec=Scenario)
    scenario.id = id_val
    scenario.warnings = Mock(__len__=Mock(return_value=0))
    return scenario


def data_sent(mock_apply_data):
    """The (scenario, data) pairs passed to apply_data."""
    return mock_apply_data.call_args[0][0]


@patch("pyetm.models.scenarios.apply_data", return_value=[])
@patch("pyetm.models.scenario.Scenario.new")
class TestCreateManyData:
    """Inputs, curves and sortables go to apply_data, for the scenarios that have them."""

    def test_user_values_are_applied(self, mock_new, mock_apply_data):
        first, second = saved(10), saved(20)
        mock_new.side_effect = [first, second]
        params: list[ScenarioCreationParams] = [
            {"title": "S1", "area_code": "nl", "end_year": 2050, "user_values": {"a": 1000}},
            {"title": "S2", "area_code": "nl", "end_year": 2050, "user_values": {"a": 2000}},
        ]

        result = Scenarios.create_many(params, client=Mock())

        assert len(result.items) == 2
        pairs = data_sent(mock_apply_data)
        assert [scenario for scenario, _ in pairs] == [first, second]
        assert pairs[0][1]["user_values"] == {"a": 1000}
        assert pairs[1][1]["user_values"] == {"a": 2000}

    def test_only_scenarios_with_data_are_applied(self, mock_new, mock_apply_data):
        first, second, third = saved(1), saved(2), saved(3)
        mock_new.side_effect = [first, second, third]
        params: list[ScenarioCreationParams] = [
            {"title": "S1", "area_code": "nl", "end_year": 2050, "user_values": {"a": 1}},
            {"title": "S2", "area_code": "nl", "end_year": 2050},
            {"title": "S3", "area_code": "nl", "end_year": 2050, "user_values": {"a": 3}},
        ]

        result = Scenarios.create_many(params, client=Mock())

        assert len(result.items) == 3
        assert [scenario for scenario, _ in data_sent(mock_apply_data)] == [first, third]

    def test_curves_and_sortables_are_applied(self, mock_new, mock_apply_data):
        mock_new.return_value = saved()
        curve = pd.Series([1] * 8760)
        sortables = {"forecast_storage": ["item1"]}
        params: list[ScenarioCreationParams] = [
            {
                "title": "S1",
                "area_code": "nl",
                "end_year": 2050,
                "user_values": {"a": 1},
                "custom_curves": {"solar_profile": curve, "wind_profile": curve},
                "sortables": sortables,
            },
        ]

        Scenarios.create_many(params, client=Mock())

        data = data_sent(mock_apply_data)[0][1]
        assert data["user_values"] == {"a": 1}
        assert set(data["custom_curves"]) == {"solar_profile", "wind_profile"}
        assert data["sortables"] == sortables

    def test_data_warnings_are_collected(self, mock_new, mock_apply_data):
        mock_new.side_effect = [saved(1), saved(2)]
        mock_apply_data.return_value = ["Invalid input", "Invalid curve"]
        params: list[ScenarioCreationParams] = [
            {"title": "S1", "area_code": "nl", "end_year": 2050, "user_values": {"x": 1}},
            {"title": "S2", "area_code": "nl", "end_year": 2050, "custom_curves": {"c": [1]}},
        ]

        result = Scenarios.create_many(params, client=Mock())

        assert len(result.items) == 2
        messages = " ".join(w.message for w in result.warnings)
        assert "Invalid input" in messages and "Invalid curve" in messages


class TestCreateManyCreation:
    @patch("pyetm.models.scenario.Scenario.new")
    def test_area_code_and_end_year_defaults(self, mock_new):
        mock_new.return_value = saved()

        Scenarios.create_many([{"title": "S1", "private": True}], area_code="nl", end_year=2050, client=Mock())

        mock_new.assert_called_once_with(
            title="S1", area_code="nl", end_year=2050, private=True, client=mock_new.call_args.kwargs["client"]
        )

    @patch("pyetm.models.scenarios.Scenario.load")
    def test_source_copies_the_saved_scenario(self, mock_load):
        source = saved(100)
        source.copy_with_preset.return_value = saved(101)
        mock_load.return_value = source

        result = Scenarios.create_many([{"title": "From source", "source": 100, "private": False}], client=Mock())

        assert result.items == [source.copy_with_preset.return_value]
        assert mock_load.call_args.args == (100,)
        source.copy_with_preset.assert_called_once_with(title="From source", private=False)

    @patch("pyetm.models.scenario.Scenario.new")
    def test_missing_title_or_area_becomes_a_warning(self, mock_new):
        result = Scenarios.create_many(
            [{"area_code": "nl", "end_year": 2050}, {"title": "No area"}], client=Mock()
        )

        assert result.items == []
        mock_new.assert_not_called()
        messages = " ".join(w.message for w in result.warnings)
        assert "Missing required 'title'" in messages
        assert "Missing area_code or end_year" in messages

    @patch("pyetm.models.scenario.Scenario.new")
    def test_partial_creation_failure(self, mock_new):
        first, third = saved(1), saved(3)
        mock_new.side_effect = [first, ScenarioError("Creation failed"), third]
        params: list[ScenarioCreationParams] = [
            {"title": f"S{i}", "area_code": "nl", "end_year": 2050} for i in (1, 2, 3)
        ]

        result = Scenarios.create_many(params, client=Mock())

        assert result.items == [first, third]
        assert "Creation failed" in " ".join(w.message for w in result.warnings)

    @patch("pyetm.models.base.get_error_policy")
    @patch("pyetm.models.scenarios.apply_data", return_value=["Failed to apply data"])
    @patch("pyetm.models.scenario.Scenario.new")
    def test_safe_mode_raises_on_data_errors(self, mock_new, _apply, mock_get_policy):
        from pyetm.models.error_policy import ErrorMode, ErrorPolicy

        mock_get_policy.return_value = ErrorPolicy(mode=ErrorMode.SAFE)
        mock_new.return_value = saved()

        with pytest.raises(RuntimeError, match="(?i)failed to apply data"):
            Scenarios.create_many(
                [{"title": "S1", "area_code": "nl", "end_year": 2050, "user_values": {"x": 1}}],
                client=Mock(),
            )
