# User Guide

Welcome to the pyetm User Guide! This section provides in-depth guidance on using pyetm effectively for your energy modeling workflows.

## What You'll Learn

This guide covers all major features of pyetm, from basic scenario creation to advanced batch processing and data analysis.

## Guide Contents

### [Working with Scenarios](scenarios.md)
Learn how to create, load, modify, and save ETM scenarios programmatically.

**Topics covered:**

- Creating new scenarios
- Loading existing scenarios
- Copying and modifying scenarios
- Scenario properties and metadata
- Error handling and validation

### [Managing Inputs](inputs.md)
Master input handling, including bulk updates, validation, and working with Excel/CSV files.

**Topics covered:**

- Setting individual inputs
- Bulk input updates
- Input validation and bounds
- Reading/writing Excel and CSV files
- Input file formats

### [Exports and Queries](exports.md)
Query scenario results and export data in various formats.

**Topics covered:**

- Querying GQL results
- Exporting hourly curves
- Annual exports
- Custom curve uploads
- Data visualization

### [Sessions and Authentication](sessions.md)
Understand authentication, sessions, and managing multiple scenarios.

**Topics covered:**

- Authentication with API tokens
- Session management
- Working with multiple scenarios
- Public vs. private scenarios
- Saved scenarios and collections

### [Advanced Usage](advanced.md)
Advanced patterns for power users and automation workflows.

**Topics covered:**

- Batch processing scenarios
- Scenario packing and unpacking
- Async operations
- Custom client configuration
- Integration with other tools

## Common Workflows

### Quick Reference: Create and Query a Scenario

```python
from pyetm import Scenario

# Create scenario
scenario = Scenario.create(area_code="nl", end_year=2050)

# Modify inputs
scenario.user_values = {
    "number_of_energy_power_solar_pv_solar_radiation": 50000000,
}

# Query results
results = scenario.query_results({
    "dashboard_renewability",
    "dashboard_co2_emissions",
})
```

### Quick Reference: Load and Export Data

```python
from pyetm import Scenario

# Load existing scenario
scenario = Scenario.from_scenario_id(123456)

# Export hourly curves
curves = scenario.fetch_hourly_electricity_curves()
curves.to_csv("electricity_curves.csv")

# Query specific outputs
cost = scenario.query_result("dashboard_total_costs")
print(f"Total costs: {cost} billion €")
```

### Quick Reference: Batch Process Scenarios

```python
from pyetm import Scenario

scenarios = []

# Create multiple scenarios
for solar_capacity in [10000, 50000, 100000]:
    scenario = Scenario.create(area_code="nl", end_year=2050)
    scenario.user_values = {
        "number_of_energy_power_solar_pv_solar_radiation": solar_capacity,
    }
    scenarios.append(scenario)

# Collect results
for i, scenario in enumerate(scenarios):
    renewability = scenario.renewable_percentage
    print(f"Scenario {i+1}: {renewability:.1f}% renewable")
```

## Getting Help

- **API Reference**: See the [API Reference](../api/index.md) for detailed documentation of all classes and methods
- **Examples**: Check the `examples/` directory in the repository for complete working examples
- **Issues**: Report problems or ask questions on [GitHub Issues](https://github.com/quintel/pyetm/issues)

## Next Steps

Start with [Working with Scenarios](scenarios.md) to learn the fundamentals of scenario management in pyetm.
