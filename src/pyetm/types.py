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
    "merit_order",
    "electricity_price",
    "heat_network",
    "agriculture_heat",
    "household_heat",
    "buildings_heat",
    "hydrogen",
    "network_gas",
    "residual_load",
    "hydrogen_integral_cost",
]
