# Managing Inputs

Learn how to work with scenario inputs, including bulk updates, validation, and file-based workflows.

## Overview

ETM scenarios are controlled by thousands of input parameters. pyetm provides several ways to manage these inputs efficiently.

## Setting Inputs

### Single Input

Set a single input value:

```python
scenario.user_values = {
    "number_of_energy_power_solar_pv_solar_radiation": 50000000,
}
```

### Multiple Inputs

Set many inputs at once:

```python
scenario.user_values = {
    "number_of_energy_power_solar_pv_solar_radiation": 50000000,
    "number_of_energy_power_wind_turbine_inland": 5000,
    "number_of_energy_power_wind_turbine_offshore": 1000,
    "transport_car_using_electricity_share": 75.0,
    "households_solar_pv_solar_radiation_market_penetration": 50.0,
}
```

## Input Files

### Excel Format

pyetm supports reading inputs from Excel files:

```python
from pyetm import Scenario

# Create scenario from Excel file
scenario = Scenario.create(area_code="nl", end_year=2050)
scenario.load_inputs_from_excel("path/to/inputs.xlsx")
```

**Excel file format:**

| slider_key | value |
|------------|-------|
| number_of_energy_power_solar_pv_solar_radiation | 50000000 |
| transport_car_using_electricity_share | 75.0 |

### CSV Format

Load inputs from CSV:

```python
scenario.load_inputs_from_csv("path/to/inputs.csv")
```

**CSV file format:**

```csv
slider_key,value
number_of_energy_power_solar_pv_solar_radiation,50000000
transport_car_using_electricity_share,75.0
```

### Export Inputs

Save current inputs to a file:

```python
# Export to Excel
scenario.export_inputs_to_excel("outputs/my_inputs.xlsx")

# Export to CSV
scenario.export_inputs_to_csv("outputs/my_inputs.csv")
```

## Input Discovery

### Finding Input Keys

ETM has thousands of inputs. To find the right key:

1. **Use the ETM interface**: Open a scenario in the web interface, modify a slider, and check the URL parameters
2. **Use the API documentation**: See [ETM API docs](https://docs.energytransitionmodel.com/api/intro) for input listings
3. **Inspect existing scenarios**: Load a scenario and examine its `user_values`

### Example: Finding Transport Inputs

```python
# Load a scenario with transport changes
scenario = Scenario.from_scenario_id(123456)

# Find all transport-related inputs
transport_inputs = {
    k: v for k, v in scenario.user_values.items()
    if "transport" in k.lower()
}

for key in transport_inputs:
    print(key)
```

## Input Validation

### Bounds Checking

Inputs have minimum and maximum bounds. pyetm validates these automatically:

```python
try:
    scenario.user_values = {
        "number_of_energy_power_solar_pv_solar_radiation": -1000,  # Invalid!
    }
except ValueError as e:
    print(f"Validation error: {e}")
```

### Type Checking

Inputs must be numeric:

```python
# Valid
scenario.user_values = {
    "transport_car_using_electricity_share": 75.0,
}

# Invalid - will raise error
scenario.user_values = {
    "transport_car_using_electricity_share": "high",  # Must be numeric!
}
```

## Bulk Input Operations

### Scenario Packing

For working with many scenarios and inputs, use scenario packing:

```python
from pyetm.models import ScenarioPacker

# Create packer with input specification
inputs_config = {
    "number_of_energy_power_solar_pv_solar_radiation": [10000, 50000, 100000],
    "transport_car_using_electricity_share": [25.0, 50.0, 75.0],
}

packer = ScenarioPacker(
    base_scenario_id=123456,
    inputs=inputs_config,
)

# Generate all combinations
scenarios = packer.create_all_scenarios()
print(f"Created {len(scenarios)} scenario variants")
```

See [Advanced Usage](advanced.md#scenario-packing) for more on scenario packing.

### Input Templates

Create reusable input templates:

```python
# Define template
high_renewable_template = {
    "number_of_energy_power_solar_pv_solar_radiation": 80000000,
    "number_of_energy_power_wind_turbine_offshore": 2000,
    "number_of_energy_power_wind_turbine_inland": 8000,
    "transport_car_using_electricity_share": 80.0,
    "households_solar_pv_solar_radiation_market_penetration": 70.0,
}

# Apply to scenarios
for scenario in scenarios:
    scenario.user_values = high_renewable_template
```

## Input Categories

ETM inputs are organized by category:

### Energy Supply

```python
energy_supply_inputs = {
    "number_of_energy_power_solar_pv_solar_radiation": 50000000,
    "number_of_energy_power_wind_turbine_offshore": 1000,
    "number_of_energy_power_ultra_supercritical_coal": 5,
    "number_of_energy_power_combined_cycle_gas": 10,
}
```

### Demand (Transport, Households, Industry)

```python
demand_inputs = {
    # Transport
    "transport_car_using_electricity_share": 75.0,
    "transport_truck_using_electricity_share": 30.0,

    # Households
    "households_solar_pv_solar_radiation_market_penetration": 50.0,
    "households_heat_pump_air_water_electricity_market_penetration": 40.0,

    # Industry
    "industry_chemicals_other_process_electricity_share": 20.0,
}
```

### Flexibility & Storage

```python
flexibility_inputs = {
    "capacity_of_energy_power_hv_network_electricity": 15000,
    "capacity_of_energy_flexibility_hv_opac_electricity": 5000,
    "volume_of_imported_hydrogen": 100.0,
}
```

## Working with Input Units

Different inputs use different units. Common units include:

- **Capacity**: Number of units (e.g., `number_of_energy_power_solar_pv_solar_radiation`)
- **Percentages**: 0-100 (e.g., `transport_car_using_electricity_share`)
- **Market penetration**: 0-100% (e.g., `households_solar_pv_solar_radiation_market_penetration`)
- **Energy**: PJ, TWh depending on context

Always check the ETM interface for the correct unit for each input.

## Best Practices

1. **Use descriptive filenames** for input files (e.g., `high_solar_2050_inputs.xlsx`)
2. **Version control input files** to track changes over time
3. **Validate inputs** before batch operations
4. **Document assumptions** in comments or separate documentation
5. **Test with small input sets** before scaling up

## Next Steps

- [Exports and Queries](exports.md) - Learn how to query results
- [Advanced Usage](advanced.md) - Batch processing and scenario packing
- [API Reference](../api/) - Complete API documentation
