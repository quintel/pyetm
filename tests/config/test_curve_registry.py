"""Tests for the curve registry (static name map, no engine dialect)."""

import pytest

from pyetm.config import curve_registry as registry


class TestAcceptAlias:
    @pytest.mark.parametrize(
        "old,canonical",
        [
            ("merit_order", "electricity_profiles"),
            ("heat_network", "district_heating_profiles"),
            ("hydrogen", "hydrogen_profiles"),
            ("network_gas", "network_gas_profiles"),
        ],
    )
    def test_aliases_resolve(self, old, canonical):
        assert registry.accept_alias(old) == canonical

    def test_canonical_unchanged(self):
        assert registry.accept_alias("electricity_profiles") == "electricity_profiles"

    def test_unknown_passes_through(self):
        assert registry.accept_alias("some_custom_thing") == "some_custom_thing"

    def test_deprecated_warns(self, caplog):
        with caplog.at_level("WARNING"):
            registry.accept_alias("merit_order")
        assert any("deprecated" in r.message for r in caplog.records)


class TestBuildPath:
    @pytest.mark.parametrize(
        "name,expected",
        [
            # renamed curves are requested under their universal old name
            ("electricity_profiles", "/scenarios/42/curves/merit_order.csv"),
            ("district_heating_profiles", "/scenarios/42/curves/heat_network.csv"),
            ("hydrogen_profiles", "/scenarios/42/curves/hydrogen.csv"),
            ("network_gas_profiles", "/scenarios/42/curves/network_gas.csv"),
            # old names accepted directly resolve to the same wire path
            ("merit_order", "/scenarios/42/curves/merit_order.csv"),
            ("heat_network", "/scenarios/42/curves/heat_network.csv"),
            # unchanged curves keep their name
            ("residual_load", "/scenarios/42/curves/residual_load.csv"),
            # unknown identifiers pass through unchanged
            ("custom_thing", "/scenarios/42/curves/custom_thing.csv"),
        ],
    )
    def test_curves_endpoint(self, name, expected):
        assert registry.build_path(42, name) == expected

    @pytest.mark.parametrize(
        "name,expected",
        [
            ("electricity_capacities", "/scenarios/42/electricity_capacities"),
            ("district_heating_capacities", "/scenarios/42/district_heating_capacities"),
            ("hydrogen_capacities", "/scenarios/42/hydrogen_capacities"),
            ("network_gas_capacities", "/scenarios/42/network_gas_capacities"),
        ],
    )
    def test_capacities_use_export_endpoint(self, name, expected):
        assert registry.build_path(42, name) == expected


class TestMetadata:
    def test_canonical_names_complete(self):
        names = registry.canonical_names()
        assert "electricity_profiles" in names
        assert "district_heating_capacities" in names
        assert "heat_network_profiles" not in names  # the wrong guessed name is not canonical

    def test_curve_type_for(self):
        assert registry.curve_type_for("merit_order") == "merit_curve"
        assert registry.curve_type_for("electricity_capacities") == "capacity_curve"
        assert registry.curve_type_for("unknown") == "output_curve"

    def test_carrier_to_canonical(self):
        assert registry.carrier_to_canonical() == {
            "electricity": "electricity_profiles",
            "heat": "district_heating_profiles",
            "hydrogen": "hydrogen_profiles",
            "methane": "network_gas_profiles",
        }
