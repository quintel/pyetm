"""Type definitions for pyetm curve and export types."""

from typing import Literal

# Carrier types for hourly output curves
CarrierType = Literal["electricity", "heat", "hydrogen", "methane"]

# Annual export types
AnnualExportType = Literal[
    "production_parameters",
    "energy_flow",
    "energy_flow_present",
    "molecule_flow",
    "sankey",
    "storage_parameters",
    "costs_parameters",
]

# Hourly curve types
HourlyCurveType = Literal[
    "electricity_profiles",
    "electricity_price",
    "heat_network_profiles",
    "agriculture_heat",
    "household_heat",
    "buildings_heat",
    "hydrogen_profiles",
    "network_gas_profiles",
    "residual_load",
    "hydrogen_integral_cost",
    "electricity_capacities",
    "heat_network_capacities",
    "hydrogen_capacities",
    "network_gas_capacities",
]
