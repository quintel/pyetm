"""Tests for validation utilities."""

import pytest

from pyetm.validators import (
    validate_carrier_type,
    validate_hourly_curve_names,
    validate_export_names,
)


class TestValidateCarrierType:
    """Tests for validate_carrier_type function."""

    def test_valid_electricity(self):
        """Test that 'electricity' is accepted."""
        result = validate_carrier_type("electricity")
        assert result == "electricity"

    def test_valid_heat(self):
        """Test that 'heat' is accepted."""
        result = validate_carrier_type("heat")
        assert result == "heat"

    def test_valid_hydrogen(self):
        """Test that 'hydrogen' is accepted."""
        result = validate_carrier_type("hydrogen")
        assert result == "hydrogen"

    def test_valid_methane(self):
        """Test that 'methane' is accepted."""
        result = validate_carrier_type("methane")
        assert result == "methane"

    def test_invalid_carrier_type(self):
        """Test that invalid carrier type raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            validate_carrier_type("invalid")

        error_message = str(exc_info.value)
        assert "Invalid carrier type 'invalid'" in error_message
        assert "electricity" in error_message
        assert "heat" in error_message
        assert "hydrogen" in error_message
        assert "methane" in error_message

    def test_invalid_carrier_type_case_sensitive(self):
        """Test that carrier type validation is case-sensitive."""
        with pytest.raises(ValueError) as exc_info:
            validate_carrier_type("Electricity")

        assert "Invalid carrier type 'Electricity'" in str(exc_info.value)

    def test_invalid_empty_string(self):
        """Test that empty string raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            validate_carrier_type("")

        assert "Invalid carrier type ''" in str(exc_info.value)


class TestValidateExportNames:
    """Tests for validate_export_names function."""

    def test_single_string_auto_conversion(self):
        """Test that single string is converted to list."""
        result = validate_export_names("energy_flow")
        assert result == ["energy_flow"]

    def test_list_of_valid_exports(self):
        """Test that list of valid exports is accepted."""
        exports = ["energy_flow", "sankey", "production_parameters"]
        result = validate_export_names(exports)
        assert result == exports

    def test_all_valid_export_types(self):
        """Test that all valid export types are accepted."""
        all_exports = [
            "production_parameters",
            "energy_flow",
            "energy_flow_present",
            "molecule_flow",
            "sankey",
            "storage_parameters",
            "costs_parameters",
        ]
        result = validate_export_names(all_exports)
        assert result == all_exports

    def test_invalid_export_name(self):
        """Test that invalid export name raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            validate_export_names("invalid_export")

        error_message = str(exc_info.value)
        assert "Invalid export names: ['invalid_export']" in error_message
        assert "production_parameters" in error_message
        assert "energy_flow" in error_message

    def test_mixed_valid_and_invalid_exports(self):
        """Test that mixed valid/invalid exports raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            validate_export_names(["energy_flow", "invalid1", "sankey", "invalid2"])

        error_message = str(exc_info.value)
        assert "invalid1" in error_message
        assert "invalid2" in error_message

    def test_empty_list(self):
        """Test that empty list is accepted."""
        result = validate_export_names([])
        assert result == []

    def test_single_invalid_string(self):
        """Test that single invalid string raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            validate_export_names("not_an_export")

        assert "Invalid export names: ['not_an_export']" in str(exc_info.value)


class TestValidateCurveNames:
    """Tests for validate_hourly_curve_names function."""

    def test_single_string_auto_conversion(self):
        """Test that single string is converted to list."""
        result = validate_hourly_curve_names("electricity_profiles")
        assert result == ["electricity_profiles"]

    def test_list_of_valid_curves(self):
        """Test that list of valid curves is accepted."""
        curves = ["electricity_profiles", "electricity_price", "district_heating_profiles"]
        result = validate_hourly_curve_names(curves)
        assert result == curves

    def test_all_valid_curve_types(self):
        """Test that all valid curve types are accepted."""
        all_curves = [
            "electricity_profiles",
            "electricity_price",
            "district_heating_profiles",
            "agriculture_heat",
            "household_heat",
            "buildings_heat",
            "hydrogen_profiles",
            "network_gas_profiles",
            "residual_load",
            "hydrogen_integral_cost",
            "electricity_capacities",
            "district_heating_capacities",
            "hydrogen_capacities",
            "network_gas_capacities",
        ]
        result = validate_hourly_curve_names(all_curves)
        assert result == all_curves

    def test_legacy_and_old_names_normalized(self):
        """Legacy/old engine names are accepted and normalized to canonical keys."""
        result = validate_hourly_curve_names(
            ["merit_order", "heat_network", "network_gas", "hydrogen"]
        )
        assert result == [
            "electricity_profiles",
            "district_heating_profiles",
            "network_gas_profiles",
            "hydrogen_profiles",
        ]

    def test_invalid_curve_name(self):
        """Test that invalid curve name raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            validate_hourly_curve_names("invalid_curve")

        error_message = str(exc_info.value)
        assert "Invalid curve names: ['invalid_curve']" in error_message
        assert "electricity_profiles" in error_message
        assert "electricity_price" in error_message

    def test_mixed_valid_and_invalid_curves(self):
        """Test that mixed valid/invalid curves raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            validate_hourly_curve_names(
                ["electricity_profiles", "bad_curve", "electricity_price", "another_bad"]
            )

        error_message = str(exc_info.value)
        assert "bad_curve" in error_message
        assert "another_bad" in error_message

    def test_empty_list(self):
        """Test that empty list is accepted."""
        result = validate_hourly_curve_names([])
        assert result == []

    def test_single_invalid_string(self):
        """Test that single invalid string raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            validate_hourly_curve_names("not_a_curve")

        assert "Invalid curve names: ['not_a_curve']" in str(exc_info.value)

    def test_case_sensitive_validation(self):
        """Test that curve name validation is case-sensitive."""
        with pytest.raises(ValueError) as exc_info:
            validate_hourly_curve_names("Merit_Order")

        assert "Invalid curve names: ['Merit_Order']" in str(exc_info.value)
