## Inputs (Slider Settings)

Inputs represent slider settings in the ETM interface. Each input has a key, unit, default value, and optionally a user-set value.

### Quick Reference

| Operation | Method | Returns |
|-----------|--------|---------|
| Fetch | `scenario.inputs` | `Inputs` collection |
| View single | `scenario.inputs[key]` | `Input` object |
| View all | `scenario.inputs.to_dataframe()` | DataFrame |
| Update | `scenario.update_user_values(data)` | Updated scenario |
| Remove | `scenario.remove_user_values(keys)` | Updated scenario |

### Fetching Inputs

```python
from pyetm import Scenario

scenario = Scenario.load(123456)

# Access inputs (auto-fetches on first access)
inputs = scenario.inputs

# List all input keys
all_keys = inputs.keys()
```

### Viewing Inputs

```python
# Access a single input
input_obj = inputs["investment_costs_co2_ccs"]
print(f"Value: {input_obj.merged_value}")  # User value or default
print(f"Unit: {input_obj.unit}")
print(f"Min: {input_obj.min}, Max: {input_obj.max}")

# Export to DataFrame
df = inputs.to_dataframe(columns="value")  # Merged user/default values
df = inputs.to_dataframe(columns=["user", "default", "min", "max"])

print(df.head())
```

### Setting and Altering Inputs

Update locally
```python
# Invalid values are rejected, warnings are shown immediately
inputs.update({"investment_costs_co2_ccs": 50.0})
# No warnings (valid update)
```

Value not changes with bad input update
```python
inputs.update({"investment_costs_co2_ccs": -500.0})
#  [WARNING] Value error, -500.0 should be between -100.0 and 300.0
inputs.update({"nonexistent_input": 100.0})
#  [WARNING] Input 'nonexistent_input' does not exist
```

Update and upload to API immediately (validates and raises exception if invalid)
```python
scenario.update_user_values({
    "investment_costs_co2_ccs": 50.0,
    "capacity_of_energy_hydrogen_steam_methane_reformer": 100.0
})
```

Remove user values (revert to defaults)
```python
scenario.remove_user_values([
    "investment_costs_co2_ccs",
    "capacity_of_energy_hydrogen_steam_methane_reformer"
])
```

Set from DataFrame (input keys as index)
```python
df = pd.DataFrame({
    "user": [50.0]
}, index=pd.Index(["investment_costs_co2_ccs"], name="input"))
scenario.set_user_values_from_dataframe(df)
```

### Input Types and Details

**Three input types:**

- **FloatInput**: Numeric values with min/max bounds
- **BoolInput**: Boolean values (stored internally as 1.0/0.0)
- **EnumInput**: String values from a permitted list

**Key behaviors:**

- **Merged values**: `input.merged_value` returns the user value if set, otherwise the default
- **Disabled inputs**: Some inputs may be disabled by coupling settings (`disabled=True`)
- **Validation and warnings**:
    - `inputs.update()` validates locally and auto-displays warnings immediately
    - Invalid values are **rejected** (not applied) to maintain data integrity
    - Non-existent input keys trigger warnings
    - `inputs.is_valid_update()` pre-validates without side effects (returns `WarningCollector` dict)
    - `scenario.update_user_values()` validates and raises `ScenarioError` before API upload
    - Warnings auto-clear on each `update()` call to show only current issues
- **Reset values**: Set a user value to `"reset"` to clear it (reverts to default)
- **Automatic coercion**:
    - BoolInput accepts "true", "false", 1, 0, "on", "off"
    - FloatInput coerces numeric strings to numbers
    - EnumInput strips whitespace from values
