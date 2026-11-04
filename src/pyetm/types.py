"""Type definitions for pyetm curve and export types."""

from typing import Literal

# Carrier types for hourly output curves
CarrierType = Literal["electricity", "hydrogen", "heat", "methane"]

# Annual export types: An export the target engine lacks returns None with a warning.
AnnualExportType = Literal[
    "energy_flow",
    "energy_flow_present",
    "molecule_flow",
    "sankey",
    "storage_parameters",
    "costs_parameters",
    "electricity_capacities",
    "district_heating_capacities",
    "hydrogen_capacities",
    "network_gas_capacities",
]

# Hourly curve types
HourlyCurveType = Literal[
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
]
