# User Guide

Welcome to the pyetm User Guide! This guide provides comprehensive documentation for using pyetm to work with the Energy Transition Model API.

## What You'll Learn

This guide covers all major features of pyetm, from basic scenario creation to advanced workflows.

## Guide Contents

### [Working with Excel](excel.md)
Import and export scenarios using Excel files with full control over what data is included.

**Topics covered:**

- Excel file structure
- Importing scenarios with the `update` parameter
- Setting export configuration
- Exporting scenarios, inputs, curves, and outputs

### [Scenarios](scenarios.md)
Learn how to create, load, copy, and manage ETM scenarios programmatically.

**Topics covered:**

- Creating new scenarios
    - Session vs Scenario
- Loading existing scenarios
- Properties and metadata
- Copying scenarios (with and without preset links)
- Collection classes (Scenarios, Sessions)
- Bulk operations

### [Inputs](inputs.md)
Master input handling including user values, validation, and bulk updates.

**Topics covered:**

- Getting and setting inputs
- Validation and bounds
- Resetting inputs
- DataFrame integration

### [Gqueries](gqueries.md)
Query scenario results and retrieve calculated outputs.

**Topics covered:**

- Adding queries to scenarios
- Executing queries
- Retrieving results as DataFrames

### [Custom Curves](custom-curves.md)
Upload and manage custom price and demand curves.

**Topics covered:**

- Viewing attached curves
- Updating custom curves
- Listing available curves
- Checking attachment status

### [Sortables](sortables.md)
Control technology ordering in merit order and heat networks.

**Topics covered:**

- Setting sortable order
- Viewing current configuration
- Available sortables list
- DataFrame import

### [Outputs](outputs.md)
Extract and analyze scenario outputs including hourly curves and annual exports.

**Topics covered:**

- Viewing submodels (inputs, sortables, custom_curves)
- to_dataframe() methods
- Annual exports (energy_flow, sankey, etc.)
- Hourly output curves
- Carrier type filtering (electricity, heat, hydrogen, methane)

### [Advanced Usage](advanced.md)
Advanced features for power users and automation workflows.

**Topics covered:**

- Client configuration for multiple environments
- ScenarioPacker architecture
- Interpolation (creating intermediate scenarios)
- Bulk operations (create_many, load_many)
- User management (sharing scenarios)

## Common Workflows

### Quick Reference: Create and Modify a Scenario

```python
from pyetm import Scenario

# Create scenario
scenario = Scenario.create(
    title="Netherlands 2050",
    area_code="nl",
    end_year=2050
)

# Set inputs
scenario.update_user_values({
    "flh_of_energy_power_solar_pv_solar_radiation": 1000,
    "households_heater_district_heating_steam_hot_water_share": 30.0,
})

# Add queries and execute
scenario.add_queries([
    "dashboard_renewability",
    "dashboard_co2_emissions",
])
scenario.execute_queries()

# Get results
results = scenario.results(columns=["future"])
print(results)
```

### Quick Reference: Excel Round-Trip

```python
from pyetm import Scenarios

# Import scenarios
scenarios = Scenarios.from_excel("scenarios.xlsx", update=False)

# Modify in Python
for scenario in scenarios:
    scenario.update_user_values({
        "flh_of_energy_power_solar_pv_solar_radiation": 1000
    })

# Export back to Excel
scenarios.to_excel(
    "modified_scenarios.xlsx",
    include_inputs=True,
    carriers=["electricity"]
)
```

### Quick Reference: Batch Processing

```python
from pyetm import Scenarios

# Create multiple scenarios
scenario_configs = [
    {"title": "Low Solar", "user_values": {"flh_of_energy_power_solar_pv_solar_radiation": 800}},
    {"title": "Medium Solar", "user_values": {"flh_of_energy_power_solar_pv_solar_radiation": 900}},
    {"title": "High Solar", "user_values": {"flh_of_energy_power_solar_pv_solar_radiation": 1100}},
]

scenarios = Scenarios.create_many(
    scenario_configs,
    area_code="nl2023",
    end_year=2050
)

# Add queries to all
for scenario in scenarios:
    scenario.add_queries(["dashboard_renewability"])
    scenario.execute_queries()

# Export comparison
scenarios.to_excel("comparison.xlsx")
```

## Getting Help

- **API Reference**: See the [API Reference](../api/index.md) for detailed documentation of all classes and methods
- **Examples**: Check the user guide sections for complete working examples
- **Issues**: Report problems or ask questions on [GitHub Issues](https://github.com/quintel/pyetm/issues)

## Next Steps

Start with [Working with Excel](excel.md) if you want to import/export scenarios, or jump to [Scenarios](scenarios.md) to learn about scenario management fundamentals.
