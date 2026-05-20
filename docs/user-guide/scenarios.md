# Working with Scenarios

Scenarios are the core concept in pyetm. This guide explains how to create, load, modify, and manage ETM scenarios programmatically.

## Creating Scenarios

### Basic Creation

Create a new scenario for a specific region and end year:

```python
from pyetm import Scenario

# Create a scenario for the Netherlands in 2050
scenario = Scenario.create(
    title="Netherlands 2050",
    area_code="nl2023",
    end_year=2050
)

print(f"Created scenario {scenario.id}")
print(f"Session URL: {scenario.session.url}")
```

### With Custom Settings

You can specify additional parameters when creating a scenario:

```python
from pyetm import Scenario, Client

# Initialize authenticated client
client = Client.from_env()

# Create scenario with custom settings
scenario = Scenario.create(
    title="High Renewables Scenario",
    area_code="nl2023",
    end_year=2050,
    client=client,
    private=False,
)
```

**Available parameters:**

- `title` (required): Scenario title
- `area_code` (required if not providing session_id): Region code (e.g., "nl2023", "de", "uk2050")
- `end_year` (required if not providing session_id): Target year for the scenario (e.g., 2030, 2040, 2050)
- `session_id` (alternative to area_code+end_year): ID of existing session to save
- `client`: Authenticated client for saving
- `private`: Whether the scenario is private (default: False)

### From a Preset

Start from an existing preset scenario:

```python
# Create from preset
scenario = Scenario.from_scenario_id(
    scenario_id=123456,  # Preset scenario ID
    client=client,
)

# Modify and save as new scenario
scenario.user_values = {"input_key": 100.0}
new_scenario = scenario.save(title="Modified Preset")
```

## Loading Scenarios

### Load by ID

Load an existing scenario by its ID:

```python
from pyetm import Scenario

# Load public scenario
scenario = Scenario.from_scenario_id(123456)

# Load private scenario (requires authentication)
client = Client.from_env()
scenario = Scenario.from_scenario_id(123456, client=client)
```

### Load by URL

Load a scenario from its ETM URL:

```python
url = "https://energytransitionmodel.com/scenarios/123456"
scenario = Scenario.from_url(url)
```

### Load Multiple Scenarios

For batch processing, load multiple scenarios:

```python
scenario_ids = [123456, 123457, 123458]
scenarios = [Scenario.from_scenario_id(sid) for sid in scenario_ids]

# Process all scenarios
for scenario in scenarios:
    print(f"{scenario.title}: {scenario.renewable_percentage:.1f}% renewable")
```

## Modifying Scenarios

### Setting Inputs

Modify scenario inputs using the `user_values` attribute:

```python
# Set a single input
scenario.user_values = {
    "number_of_energy_power_solar_pv_solar_radiation": 50000000,
}

# Set multiple inputs
scenario.user_values = {
    "number_of_energy_power_solar_pv_solar_radiation": 50000000,
    "number_of_energy_power_wind_turbine_inland": 5000,
    "transport_car_using_electricity_share": 75.0,
}
```

### Updating Inputs

Add or update inputs without replacing existing ones:

```python
# Get current inputs
current = scenario.user_values

# Update with new values
current.update({
    "number_of_households_solar_pv_solar_radiation": 2000000,
})

# Apply updates
scenario.user_values = current
```

### Resetting Inputs

Reset specific inputs to their default values:

```python
# Reset by setting to None or removing from user_values
scenario.user_values = {
    k: v for k, v in scenario.user_values.items()
    if k != "number_of_energy_power_solar_pv_solar_radiation"
}
```

## Copying Scenarios

### Simple Copy

Create a copy of an existing scenario:

```python
from pyetm.services import copy_scenario

# Copy a scenario
new_scenario = copy_scenario(
    scenario_id=123456,
    client=client,
)
```

### Copy with Modifications

Copy and modify in one step:

```python
# Load original
original = Scenario.from_scenario_id(123456, client=client)

# Copy and modify
copy = original.copy()
copy.user_values = {
    "number_of_energy_power_solar_pv_solar_radiation": 100000000,
}

# Save as new scenario
saved_copy = copy.save(title="High Solar Variant")
```

## Scenario Properties

### Metadata

Access scenario metadata:

```python
print(f"ID: {scenario.id}")
print(f"Title: {scenario.title}")
print(f"Area: {scenario.area_code}")
print(f"End year: {scenario.end_year}")
print(f"URL: {scenario.url}")
print(f"Owner: {scenario.owner_email}")
print(f"Created: {scenario.created_at}")
print(f"Updated: {scenario.updated_at}")
```

### Key Results

Access commonly-used results directly:

```python
# Energy metrics
print(f"Renewable %: {scenario.renewable_percentage:.1f}%")
print(f"CO2 emissions: {scenario.co2_emissions:.1f} Mton")
print(f"Total demand: {scenario.total_primary_demand:.1f} PJ")

# Economic metrics
print(f"Total costs: {scenario.total_costs:.1f} billion €")
print(f"Electricity price: {scenario.electricity_price:.2f} €/kWh")

# Electricity production
print(f"Total production: {scenario.total_electricity_production:.1f} PJ")
```

### User Values

Get all custom inputs:

```python
# Get all user-modified inputs
inputs = scenario.user_values
print(f"Number of custom inputs: {len(inputs)}")

for key, value in inputs.items():
    print(f"{key}: {value}")
```

## Saving Scenarios

### Save New Scenario

Save a scenario to your account:

```python
from pyetm import Client

client = Client.from_env()

scenario = Scenario.create(
    title="My Scenario",
    area_code="nl2023",
    end_year=2050,
    client=client,
    private=False,
)
scenario.update_user_values({"input_key": 100.0})

print(f"Saved scenario ID: {scenario.id}")
```

### Update Existing Scenario

Update a saved scenario:

```python
# Load saved scenario
scenario = Scenario.from_scenario_id(123456, client=client)

# Modify
scenario.user_values = {"input_key": 200.0}

# Update (saves to same ID)
scenario.save()
```

## Error Handling

### Common Errors

Handle scenario-related errors:

```python
from pyetm.exceptions import ScenarioError

try:
    scenario = Scenario.from_scenario_id(999999)
except ScenarioError as e:
    print(f"Error loading scenario: {e}")
```

### Validation

Validate scenario state before operations:

```python
if scenario.has_errors:
    print("Scenario has errors:")
    for error in scenario.errors:
        print(f"  - {error}")
```

## Best Practices

1. **Use authentication for saved scenarios**: Always use an authenticated client when saving or loading private scenarios
2. **Check for errors**: Verify `scenario.has_errors` before relying on results
3. **Handle rate limits**: ETM API has rate limits; implement delays for batch operations
4. **Cache results**: Query results don't change unless inputs change; cache when appropriate
5. **Use descriptive titles**: Make scenarios easy to identify later

## Next Steps

- [Managing Inputs](inputs.md) - Learn about input file formats and bulk updates
- [Exports and Queries](exports.md) - Query results and export data
- [API Reference: Scenario](../api/models/scenario.md) - Complete Scenario class documentation
