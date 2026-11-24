from unittest.mock import Mock, patch

import pandas as pd

from pyetm.models.scenario import Scenario
from pyetm.models.scenario_packer import ScenarioPacker
from pyetm.models.scenarios import Scenarios


class TestScenariosFromExcel:
    """Scenarios.from_excel() holds every scenario the workbook produced."""

    def test_from_excel_all_saved_scenarios(self, monkeypatch):
        """Test that from_excel returns every scenario the packer produced."""
        saved1 = Mock(spec=Scenario)
        saved1.id = 100
        saved2 = Mock(spec=Scenario)
        saved2.id = 200
        saved3 = Mock(spec=Scenario)
        saved3.id = 300

        mock_packer = Mock(spec=ScenarioPacker)
        mock_packer._scenarios.return_value = {saved1, saved2, saved3}

        with patch.object(ScenarioPacker, "from_excel", return_value=mock_packer):
            result = Scenarios.from_excel("test.xlsx")

        # Should include all SavedScenarios
        assert len(result.items) == 3
        assert saved1 in result.items
        assert saved2 in result.items
        assert saved3 in result.items

    def test_from_excel_sorts_by_id(self, monkeypatch):
        """Test that scenarios are sorted by ID."""
        saved1 = Mock(spec=Scenario)
        saved1.id = 300
        saved2 = Mock(spec=Scenario)
        saved2.id = 100
        saved3 = Mock(spec=Scenario)
        saved3.id = 200

        mock_packer = Mock(spec=ScenarioPacker)
        mock_packer._scenarios.return_value = {saved1, saved2, saved3}

        with patch.object(ScenarioPacker, "from_excel", return_value=mock_packer):
            result = Scenarios.from_excel("test.xlsx")

        # Should be sorted by ID
        assert result.items[0].id == 100
        assert result.items[1].id == 200
        assert result.items[2].id == 300


class TestScenariosGetMethods:
    """Test Scenarios convenience methods for accessing packer data."""

    def test_get_hourly_output_curves_delegates_to_packer(self):
        """Test that get_hourly_output_curves delegates to the packer."""
        # Create mock scenarios
        scenario1 = Mock(spec=Scenario)
        scenario1.id = 1
        scenario1.identifier.return_value = "scenario_1"
        scenario1.get_hourly_curves.return_value = {}

        scenario2 = Mock(spec=Scenario)
        scenario2.id = 2
        scenario2.identifier.return_value = "scenario_2"
        scenario2.get_hourly_curves.return_value = {}

        collection = Scenarios(items=[scenario1, scenario2])

        # Mock the packer
        mock_packer = Mock(spec=ScenarioPacker)
        expected_result = {
            "merit_order": {
                "Scenario 1": pd.DataFrame(),
                "Scenario 2": pd.DataFrame(),
            }
        }
        mock_packer.hourly_output_curves.return_value = expected_result
        collection._packer = mock_packer

        # Call the method
        result = collection.get_hourly_output_curves("electricity")

        # Verify fetch was called on scenarios with new API (list of identifiers)
        scenario1.get_hourly_curves.assert_called_once_with(["electricity"])
        scenario2.get_hourly_curves.assert_called_once_with(["electricity"])

        # Verify delegation to packer
        mock_packer.hourly_output_curves.assert_called_once_with(carrier_type="electricity")
        assert result == expected_result

    def test_get_hourly_output_curves_with_filter(self):
        """Test that get_hourly_output_curves passes carrier type to packer."""
        scenario1 = Mock(spec=Scenario)
        scenario1.id = 1

        collection = Scenarios(items=[scenario1])

        # Mock the packer
        mock_packer = Mock(spec=ScenarioPacker)
        expected_result = {
            "merit_order": {
                "Scenario 1": pd.DataFrame(),
            },
            "electricity_price": {
                "Scenario 1": pd.DataFrame(),
            },
            "residual_load": {
                "Scenario 1": pd.DataFrame(),
            },
        }
        mock_packer.hourly_output_curves.return_value = expected_result
        collection._packer = mock_packer

        # Call with carrier type
        result = collection.get_hourly_output_curves("electricity")

        # Verify carrier type was passed
        mock_packer.hourly_output_curves.assert_called_once_with(carrier_type="electricity")
        assert result == expected_result

    def test_get_hourly_output_curves_resolves_the_carrier_through_a_real_packer(self):
        """The carrier names its curves; it is not a list of curve names."""
        scenario = Mock(spec=Scenario)
        scenario.id = 1
        scenario.identifier.return_value = "scenario_1"
        collection = Scenarios(items=[scenario])

        with patch(
            "pyetm.models.packables.hourly_output_curves_pack.HourlyOutputCurvesPack.to_dict_per_curve",
            return_value={},
        ) as per_curve:
            collection.get_hourly_output_curves("electricity")

        per_curve.assert_called_once_with(curves=["electricity_profiles"])

    def test_get_hourly_output_curves_triggers_fetch(self):
        """Test that get_hourly_output_curves triggers fetch on each scenario if not cached."""
        # Create mock scenarios
        scenario1 = Mock(spec=Scenario)
        scenario1.id = 1
        scenario1.identifier.return_value = "scenario_1"
        scenario1.get_hourly_curves.return_value = {
            "merit_order": pd.DataFrame({"hour": [0, 1], "value": [100, 200]}),
            "electricity_price": pd.DataFrame({"hour": [0, 1], "price": [0.12, 0.13]}),
            "residual_load": pd.DataFrame({"hour": [0, 1], "load": [500, 600]}),
        }

        scenario2 = Mock(spec=Scenario)
        scenario2.id = 2
        scenario2.identifier.return_value = "scenario_2"
        scenario2.get_hourly_curves.return_value = {
            "merit_order": pd.DataFrame({"hour": [0, 1], "value": [150, 250]}),
            "electricity_price": pd.DataFrame({"hour": [0, 1], "price": [0.14, 0.15]}),
            "residual_load": pd.DataFrame({"hour": [0, 1], "load": [550, 650]}),
        }

        collection = Scenarios(items=[scenario1, scenario2])

        # Mock the packer to return organized data
        mock_packer = Mock(spec=ScenarioPacker)
        mock_packer.hourly_output_curves.return_value = {
            "merit_order": {
                "scenario_1": pd.DataFrame(),
                "scenario_2": pd.DataFrame(),
            }
        }
        collection._packer = mock_packer

        # Call get_hourly_output_curves
        result = collection.get_hourly_output_curves("electricity")

        # Verify it triggered fetch on each individual scenario with new API
        scenario1.get_hourly_curves.assert_called_once_with(["electricity"])
        scenario2.get_hourly_curves.assert_called_once_with(["electricity"])

        # Verify packer was called
        mock_packer.hourly_output_curves.assert_called_once_with(carrier_type="electricity")

        # Verify result structure
        assert isinstance(result, dict)

    def test_get_annual_exports_delegates_to_packer(self):
        """Test that get_annual_exports delegates to the packer."""
        # Create mock scenarios
        scenario1 = Mock(spec=Scenario)
        scenario1.id = 1
        scenario2 = Mock(spec=Scenario)
        scenario2.id = 2

        collection = Scenarios(items=[scenario1, scenario2])

        # Mock the packer
        mock_packer = Mock(spec=ScenarioPacker)
        expected_result = {
            "total_co2_emissions": {
                "Scenario 1": pd.DataFrame(),
                "Scenario 2": pd.DataFrame(),
            }
        }
        mock_packer.annual_exports.return_value = expected_result
        collection._packer = mock_packer

        # Call the method
        result = collection.get_annual_exports()

        # Verify delegation
        mock_packer.annual_exports.assert_called_once_with(None)
        assert result == expected_result

    def test_get_annual_exports_with_filter(self):
        """Test that get_annual_exports passes export filter to packer."""
        scenario1 = Mock(spec=Scenario)
        scenario1.id = 1

        collection = Scenarios(items=[scenario1])

        # Mock the packer
        mock_packer = Mock(spec=ScenarioPacker)
        expected_result = {
            "total_co2_emissions": {
                "Scenario 1": pd.DataFrame(),
            }
        }
        mock_packer.annual_exports.return_value = expected_result
        collection._packer = mock_packer

        # Call with specific export
        result = collection.get_annual_exports("total_co2_emissions")

        # Verify filter was passed
        mock_packer.annual_exports.assert_called_once_with("total_co2_emissions")
        assert result == expected_result

    def test_get_methods_work_through_combine_property(self):
        """Test that methods work even when packer is created via combine property."""
        scenario1 = Mock(spec=Scenario)
        scenario1.id = 1
        scenario1.identifier.return_value = "scenario_1"
        scenario1.get_hourly_curves.return_value = {}

        collection = Scenarios(items=[scenario1])

        # Don't pre-create packer - let combine property create it
        with patch.object(ScenarioPacker, "hourly_output_curves") as mock_curves:
            expected = {"merit_order": {"Scenario 1": pd.DataFrame()}}
            mock_curves.return_value = expected

            # This should work even though _packer is None
            with patch.object(ScenarioPacker, "add"):
                result = collection.get_hourly_output_curves("electricity")

            # Packer should have been created and method called
            assert collection._packer is not None
            # Verify fetch was triggered with new API
            scenario1.get_hourly_curves.assert_called_once_with(["electricity"])


class TestScenariosFromExcelWarnings:
    """Per-row failures recorded on the scenarios must reach the caller."""

    def test_from_excel_lifts_warnings_off_the_scenarios(self):
        scenario = Scenario(id=100, title="A", area_code="nl2023", end_year=2050)
        scenario.add_warning("user_values", "Row 'A' could not be updated: 422")

        mock_packer = Mock(spec=ScenarioPacker)
        mock_packer._scenarios.return_value = [scenario]

        with patch.object(ScenarioPacker, "from_excel", return_value=mock_packer):
            result = Scenarios.from_excel("test.xlsx")

        assert len(result.warnings) == 1
        assert "could not be updated" in str(list(result.warnings)[0])

    def test_from_excel_stays_quiet_when_every_row_succeeded(self):
        scenario = Scenario(id=100, title="A", area_code="nl2023", end_year=2050)

        mock_packer = Mock(spec=ScenarioPacker)
        mock_packer._scenarios.return_value = [scenario]

        with patch.object(ScenarioPacker, "from_excel", return_value=mock_packer):
            result = Scenarios.from_excel("test.xlsx")

        assert len(result.warnings) == 0
