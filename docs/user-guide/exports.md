# Working with Exports

This guide covers accessing scenario outputs through **Hourly Output Curves** and **Annual Exports**. Both scenarios and sessions provide identical access patterns.

For detailed API signatures and parameters, see the [API Reference](../api/index.md).

## Overview

The ETM provides two main types of output data:

- **Hourly Output Curves** — 8760-hour resolution data from Merit (electricity) and Fever (heat) calculations
- **Annual Exports** — Aggregated yearly data including energy flows, production parameters, costs, and sankey diagrams as csvs

Both types use lazy loading and disk caching for performance.

## Hourly Output Curves

Hourly output curves provide time-series data at hourly resolution (8760 hours per year) from the Merit and Fever calculation engines.

### Available Curve Types

The following curves are available. **Carrier Type Alias** provides a convenient shorthand for accessing curves for that energy carrier.

| Curve Name | Description | Carrier Alias |
|------------|-------------|---------------|
| `electricity_profiles` | The load on each participant in the electricity merit order | `electricity` |
| `electricity_price` |  The hourly price of electricity according to the merit order | - |
| `electricity_capacities` | The installed/peak capacity per participant in the electricity merit order | - |
| `district_heating_profiles` | The load on each participant in the heat merit order | `heat` |
| `district_heating_capacities` | The installed/peak capacity per participant in the heat merit order | - |
| `agriculture_heat` | The load on each participant in the agriculture heat merit order  | - |
| `household_heat` | The supply and demand of heat in households, including deficits and surpluses due to buffering and time-shifting | - |
| `buildings_heat` | The supply and demand of heat in buildings, including deficits and surpluses due to buffering and time-shifting | - |
| `hydrogen_profiles` | The total demand and supply for hydrogen, with additional columns for the storage demand and supply | `hydrogen` |
| `hydrogen_capacities` | The installed/peak capacity for hydrogen participants | - |
| `network_gas_profiles` | The total demand and supply for network gas, with additional columns for the storage demand and supply. | `methane` |
| `network_gas_capacities` | The installed/peak capacity for network gas participants | - |
| `residual_load` | The residual loads of various carriers | - |
| `hydrogen_integral_cost` | The levelised costs, production costs per MWh and hourly production curve per hydrogen production technology | - |

!!! note "Deprecated curve names"
    The old curve names (`merit_order`, `heat_network`, `hydrogen`, `network_gas`) are deprecated but still supported with warnings. Please update your code to use the new names (`electricity_profiles`, `district_heating_profiles`, `hydrogen_profiles`, `network_gas_profiles`).

### Accessing Single Curves

Get a single curve by name or carrier type alias:

```python
from pyetm import Scenario

scenario = Scenario.load(123456)

# By curve name
electricity_curve = scenario.get_hourly_curve("electricity_profiles")
price_curve = scenario.get_hourly_curve("electricity_price")
capacity_curve = scenario.get_hourly_curve("electricity_capacities")

# By carrier (convenience aliases)
electricity_curve = scenario.get_hourly_curve("electricity")  # Returns electricity_profiles
heat_curve = scenario.get_hourly_curve("heat")                # Returns district_heating_profiles
h2_curve = scenario.get_hourly_curve("hydrogen")              # Returns hydrogen_profiles
gas_curve = scenario.get_hourly_curve("methane")              # Returns network_gas_profiles

print(electricity_curve)  # pandas DataFrame with 8760 rows
```

### Accessing Multiple Curves

Get multiple curves at once by passing a list of names or carrier aliases:

```python
# Mix of curve names and carrier aliases
curves = scenario.get_hourly_curves([
    "electricity",        # Alias for electricity_profiles
    "electricity_price",  # Curve name
    "heat",              # Alias for district_heating_profiles
    "hydrogen"           # Alias for hydrogen_profiles
])
# Returns: {
#   "electricity_profiles": DataFrame,
#   "electricity_price": DataFrame,
#   "district_heating_profiles": DataFrame,
#   "hydrogen_profiles": DataFrame
# }

# Access individual curves from the result
electricity_profiles = curves["electricity_profiles"]
price = curves["electricity_price"]
```

### Iterate All Curves

Loop through all available hourly curves:

```python
# Generator yielding DataFrames
for curve_df in scenario.all_hourly_output_curves():
    print(f"Shape: {curve_df.shape}")
    print(curve_df.head())
```

### Access the Collection Directly

For advanced use cases, access the underlying collection:

```python
# Get the collection object
curves = scenario.hourly_output_curves

# Check if a curve exists
if curves.is_attached("electricity_profiles"):
    electricity = scenario.get_hourly_curve("electricity_profiles")

# List all available curve names
available = curves.attached_keys()
print(available)  # ['electricity_profiles', 'electricity_price', ...]
```

### Data Structure

Each curve returns a pandas DataFrame with:

- **8760 rows** — one per hour (24 hours × 365 days)
- **Columns** — vary by curve type, typically include technology/carrier values
- **Index** — hour number (0-8759)


## Annual Exports

Annual exports provide aggregated yearly data in various formats for analysis and visualization.

### Available Export Types

| Export Name | Description |
|-------------|-------------|
| `production_parameters` | Production capacity and utilization |
| `energy_flow` | Energy flows by carrier (future year) |
| `energy_flow_present` | Energy flows by carrier (present year) |
| `molecule_flow` | Molecule/hydrogen flows |
| `sankey` | Sankey diagram data |
| `storage_parameters` | Storage capacity and parameters |
| `costs_parameters` | Cost breakdown by technology |

### Basic Access

Get a single export by name:

```python
# Get energy flow export
energy_flow = scenario.get_annual_export("energy_flow")
print(energy_flow)  # pandas DataFrame indexed by carrier

# Access specific values
agriculture_crude_oil_input = energy_flow.loc["agriculture_burner_crude_oil", "input_of_crude_oil (MJ)"]
```

### Get Multiple Exports

Retrieve several exports in one call:

```python
# Get multiple exports
exports = scenario.get_annual_exports([
    "energy_flow",
    "production_parameters",
    "costs_parameters"
])

# Access individual exports
energy = exports["energy_flow"]
```

You can also pass a single export type:

```python
# Single export (returns dict with one key)
result = scenario.get_annual_exports("sankey")
sankey_data = result["sankey"]
```

### Access the Collection Directly

For advanced use cases, access the underlying collection:

```python
# Get the collection object
exports = scenario.annual_exports

# List all available export types
print(exports.names)
# ['production_parameters', 'energy_flow', 'energy_flow_present', ...]

# Get cached export (returns None if not yet fetched)
cached = exports.get("energy_flow")

# Force refresh from API
fresh = exports.retrieve(client, scenario, "energy_flow", force_refresh=True)
```

### Data Structure

Each export returns a pandas DataFrame with:

- **Index** — carrier names, technology names, or flow labels (varies by export)
- **Columns** — metrics relevant to that export type (output, demand, capacity, cost, etc.)
- **Values** — numeric data for the future year scenario


## Lazy Loading and Caching

### Lazy Loading

Exports are fetched on-demand:

```python
# First access triggers API fetch
curves = scenario.hourly_output_curves  # Fetches metadata
merit = scenario.get_hourly_curve("merit_order")  # Fetches this curve

# Subsequent access uses cached data
merit_again = scenario.get_hourly_curve("merit_order")  # Returns cached
```

### Disk Caching

Hourly curves are automatically cached to disk for performance:

```python
# Curves are saved to: ~/.pyetm/tmp/<session_id>/<curve_name>.csv
# (or custom path via PYETM_TMP_CACHE_PATH environment variable)

# Cache persists across Python sessions
# Re-running your script reuses cached curves
```

Annual exports are cached in memory but not persisted to disk.

### Force Refresh

To fetch fresh data from the API:

```python
# For annual exports
exports = scenario.annual_exports
fresh = exports.retrieve(client, scenario, "energy_flow", force_refresh=True)
```

### Clearing Caches

Clear cached curve data when you need to free disk space or force re-fetch from the API:

```python
# Clear hourly output curves cache
removed = scenario.clear_hourly_curves_cache()
print(f"Removed {removed} hourly curve files")

# Clear custom curves cache
removed = scenario.clear_custom_curves_cache()
print(f"Removed {removed} custom curve files")

# Clear all curve caches at once
hourly, custom = scenario.clear_all_curve_caches()
print(f"Removed {hourly} hourly + {custom} custom curve files")

# Clear entire session cache (most comprehensive)
scenario.clear_session_cache()
```

All cache methods work identically on both `Scenario` and `Session` objects. Cache files are stored in `~/.pyetm/tmp/<session_id>/` (or custom path via `PYETM_TMP_CACHE_PATH` environment variable).

**Note:** Clearing caches removes files from disk but does not re-fetch data automatically. Access the curves again after clearing to trigger a fresh API fetch.
