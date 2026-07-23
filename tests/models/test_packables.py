"""Tests for packable validation methods."""

import pytest
from pyetm.models.packables.hourly_output_curves_pack import HourlyOutputCurvesPack
from pyetm.models.packables.annual_exports_pack import AnnualExportsPack


class TestHourlyCurvesPackValidation:
    """Test validation of hourly curves configuration."""

    def test_validate_curve_config_with_valid_carriers(self):
        """Valid carrier names should pass through unchanged."""
        config = ["electricity", "heat", "hydrogen", "methane"]
        valid_values, warnings = HourlyOutputCurvesPack.validate_curve_config(config)

        assert set(valid_values) == set(config)
        assert len(warnings) == 0

    def test_validate_curve_config_with_single_valid_carrier(self):
        """Single valid carrier should pass through."""
        config = ["electricity"]
        valid_values, warnings = HourlyOutputCurvesPack.validate_curve_config(config)

        assert valid_values == ["electricity"]
        assert len(warnings) == 0

    def test_validate_curve_config_with_invalid_carrier(self):
        """Invalid carrier names should be filtered with warnings."""
        config = ["electricity", "electrcityyy", "heat"]
        valid_values, warnings = HourlyOutputCurvesPack.validate_curve_config(config)

        assert set(valid_values) == {"electricity", "heat"}
        assert len(warnings) == 1
        assert "electrcityyy" in warnings[0]
        assert "Invalid" in warnings[0]

    def test_validate_curve_config_with_all_invalid_carriers(self):
        """All invalid carriers should return empty list with warnings."""
        config = ["electrcityyy", "nonsense", "invalid"]
        valid_values, warnings = HourlyOutputCurvesPack.validate_curve_config(config)

        assert valid_values == []
        assert len(warnings) == 3

    def test_validate_curve_config_with_valid_curve_names(self):
        """Valid curve names should pass through unchanged."""
        config = ["electricity_profiles", "electricity_price", "district_heating_profiles"]
        valid_values, warnings = HourlyOutputCurvesPack.validate_curve_config(config)

        assert set(valid_values) == set(config)
        assert len(warnings) == 0

    def test_validate_curve_config_with_invalid_curve_names(self):
        """Invalid curve names should be filtered with warnings."""
        config = ["electricity_profiles", "electricity_profileszzz", "invalid_curve"]
        valid_values, warnings = HourlyOutputCurvesPack.validate_curve_config(config)

        assert valid_values == ["electricity_profiles"]
        assert len(warnings) == 2
        assert any("electricity_profileszzz" in w for w in warnings)
        assert any("invalid_curve" in w for w in warnings)

    def test_validate_curve_config_with_mix_of_carriers_and_curve_names(self):
        """Mix of valid carriers and curve names should all be validated."""
        config = ["electricity", "electricity_profiles", "heat", "invalid_entry"]
        valid_values, warnings = HourlyOutputCurvesPack.validate_curve_config(config)

        assert set(valid_values) == {"electricity", "electricity_profiles", "heat"}
        assert len(warnings) == 1
        assert "invalid_entry" in warnings[0]

    def test_validate_curve_config_with_empty_list(self):
        """Empty list should return empty list with no warnings."""
        config = []
        valid_values, warnings = HourlyOutputCurvesPack.validate_curve_config(config)

        assert valid_values == []
        assert len(warnings) == 0

    def test_validate_curve_config_warning_messages_are_clear(self):
        """Warning messages should mention valid options."""
        config = ["invalid_carrier"]
        valid_values, warnings = HourlyOutputCurvesPack.validate_curve_config(config)

        assert len(warnings) == 1
        # Warning should mention what's valid
        warning_lower = warnings[0].lower()
        assert "valid" in warning_lower or "allowed" in warning_lower


class TestAnnualExportsPackValidation:
    """Test validation of annual exports configuration."""

    def test_validate_export_types_with_all_valid_types(self):
        """All valid export types should pass through."""
        config = [
            "energy_flow",
            "energy_flow_present",
            "molecule_flow",
            "sankey",
            "storage_parameters",
            "costs_parameters",
        ]
        valid_values, warnings = AnnualExportsPack.validate_export_types(config)

        assert set(valid_values) == set(config)
        assert len(warnings) == 0

    def test_validate_export_types_with_subset_of_valid_types(self):
        """Subset of valid types should pass through."""
        config = ["energy_flow", "sankey", "storage_parameters"]
        valid_values, warnings = AnnualExportsPack.validate_export_types(config)

        assert set(valid_values) == set(config)
        assert len(warnings) == 0

    def test_validate_export_types_with_invalid_type(self):
        """Invalid export types should be filtered with warnings."""
        config = ["energy_flow", "sankeyyy", "storage_parameters"]
        valid_values, warnings = AnnualExportsPack.validate_export_types(config)

        assert set(valid_values) == {"energy_flow", "storage_parameters"}
        assert len(warnings) == 1
        assert "sankeyyy" in warnings[0]
        assert "Invalid" in warnings[0]

    def test_validate_export_types_with_all_invalid(self):
        """All invalid types should return empty list with warnings."""
        config = ["invalid1", "invalid2", "nonsense"]
        valid_values, warnings = AnnualExportsPack.validate_export_types(config)

        assert valid_values == []
        assert len(warnings) == 3

    def test_validate_export_types_with_empty_list(self):
        """Empty list should return empty list with no warnings."""
        config = []
        valid_values, warnings = AnnualExportsPack.validate_export_types(config)

        assert valid_values == []
        assert len(warnings) == 0

    def test_validate_export_types_warning_messages_list_valid_options(self):
        """Warning messages should list valid export types."""
        config = ["invalid_export"]
        valid_values, warnings = AnnualExportsPack.validate_export_types(config)

        assert len(warnings) == 1
        warning = warnings[0]
        # Should mention it's invalid and list valid options
        assert "invalid_export" in warning
        assert "Valid" in warning or "valid" in warning
