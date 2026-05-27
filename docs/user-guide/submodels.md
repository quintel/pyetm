# Working with Submodels

This guide covers the four main submodels in pyetm: **Inputs**, **Queries**, **Custom Curves**, and **Sortables**. Each section shows the core operations you'll use in practice.

For detailed API signatures and parameters, see the [API Reference](../api/index.md).

---

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

---

## Queries (GQL Results)

Queries (gqueries) are Graph Query Language calculations that extract values from the ETM energy graph. Queries use a **two-phase pattern**: add queries first, then explicitly execute them.

### Quick Reference

| Operation | Method | Returns |
|-----------|--------|---------|
| Add | `scenario.add_queries(keys)` | Modified scenario |
| Execute | `scenario.execute_queries()` | Query results |
| View results | `scenario.results(columns=...)` | DataFrame |
| Check status | `gqueries.is_ready()` | Boolean |
| Remove queries | `scenario.remove_queries("query1", "query2")` | Modified scenario |
| Clear all queries | `scenario.clear_queries()` | Modified scenario |

### Adding Queries

```python
# Add queries to the scenario
scenario.add_queries([
    "total_co2_emissions",
    "total_costs_of_electricity",
    "dashboard_total_costs"
])

# Add more queries later
scenario.add_queries(["dashboard_renewability"])
```

### Executing Queries

Queries are **not executed** when you add them. You must explicitly execute:

```python
scenario.execute_queries()
```
Now results are available

### Viewing Results

```python
# Get results as DataFrame
df = scenario.results(columns="future")  # Future year only
df = scenario.results(columns="present")  # Present year only
df = scenario.results(columns=["present", "future"])  # Both years

print(df)
```

### Setting and Altering Queries

**Remove specific queries from the collection:**

```python
# Remove queries you no longer need
scenario.remove_queries("total_co2_emissions", "dashboard_renewability")
```

**Clear all queries from the collection:**

```python
scenario.clear_queries()  # Also clears any accumulated warnings
```

### Quirks and Special Behaviors

**Two-phase execution:**
```python
scenario.add_queries(["total_co2_emissions"])
# Queries are NOT executed yet - results are None

scenario.execute_queries()
# NOW they're executed - results available
```

**Partial results on errors:**
- If some queries are invalid, valid queries still return results with warnings
- The system automatically retries with valid queries after showing warnings
- Warnings auto-clear on the next `execute_queries()` call to show only current issues

**Curve vs scalar queries:**
- Most queries return scalar values (single numbers)
- Some queries return 8760-hour arrays (e.g., electricity price curves)

**Lazy execution:**
- Queries start with `None` values
- Only populated after `execute_queries()` is called

**Warning behavior:**
- Warnings are not shown or added when calling 'add_queries()'
- Invalid queries trigger warnings during `execute_queries()` that are **auto-displayed immediately**
- Warnings auto-clear at the start of each `execute_queries()` call to show only current issues
- `remove_queries()` validates and auto-displays warnings for non-existent query keys
- `clear_queries()` also clears accumulated warnings

---

## Custom Curves (Hourly Profiles)

Custom curves are 8760-hour time series that override default profiles for technologies like solar PV production, EV charging patterns, or heat demand.

### Quick Reference

| Operation | Method | Returns |
|-----------|--------|---------|
| Fetch all | `scenario.custom_curves` | `CustomCurves` collection |
| Fetch single | `scenario.custom_curve_series(key)` | pandas Series (8760 values) |
| Check attached | `curves.is_attached(key)` | Boolean |
| Iterate all | `scenario.custom_curves_series()` | Generator of Series |
| Validate | `curves.is_valid_update(data)` | Dict of warnings |
| Update locally | `curves.update(data)` | Auto-displays warnings |
| Update API | `scenario.update_custom_curves(curves)` | Updated scenario |

### Fetching Custom Curves

```python
# Access custom curves collection
curves = scenario.custom_curves

# Get a single curve as pandas Series
solar_curve = scenario.custom_curve_series("weather/solar_pv_profile_1")
print(solar_curve.shape)  # (8760,)

# Iterate through all attached curves
for curve_key, series in scenario.custom_curves_series():
    print(f"{curve_key}: mean={series.mean():.2f}")
```

### Viewing Custom Curves

```python
# List all attached curve keys
attached_keys = list(curves.attached_keys())

# Check if a specific curve is attached
if curves.is_attached("weather/solar_pv_profile_1"):
    print("Solar curve attached")

# Get curve contents (fetches from API if not cached)
curve_data = curves.get_contents(scenario, "weather/solar_pv_profile_1")
# Returns pandas Series with 8760 values
```

### Setting and Altering Custom Curves

```python
# Create a custom curve (8760 hourly values)
import pandas as pd
import numpy as np

# Update curves locally WITH validation and warning display
curves = scenario.custom_curves

solar_curve = pd.Series(np.ones(8760) * 0.5)
curves.update({"weather/solar_pv_profile_1": solar_curve})
# No warnings (valid 8760-hour curve)

short_curve = pd.Series(np.ones(100))
curves.update({"weather/solar_pv_profile_1": short_curve})
#  weather/solar_pv_profile_1: Curve must have 8760 values, found 100
# Curve file NOT updated (keeps old data)

curves.update({"nonexistent_curve": solar_curve})
#  nonexistent_curve: Curve 'nonexistent_curve' is not attached to scenario

# Create CustomCurves from DataFrame for API upload
df = pd.DataFrame({
    "weather/solar_pv_profile_1": np.ones(8760) * 0.5,
    "weather/wind_offshore_baseline": np.random.random(8760)
})

new_curves = CustomCurves._from_dataframe(df, scenario_id=scenario.session.id)

# Validate before upload
validation_errors = new_curves.validate_for_upload()
if not validation_errors:
    # Upload to API immediately (default behavior)
    scenario.update_custom_curves(new_curves)
else:
    for key, warnings in validation_errors.items():
        print(f"{key}: {warnings}")
```

### Quirks and Special Behaviors

**8760-hour requirement:**
- Every curve must have exactly 8760 values (one per hour of the year)
- Validation will warn if length != 8760 but proceeds with available data
- API upload will fail if values are missing

**File-based storage:**
- Curves are saved to `/tmp/{scenario_id}/{curve_key}.csv`
- Prevents memory issues when working with hundreds of scenarios
- Files use sanitized keys: `curve.key.replace('/', '-')`

**Lazy loading:**
- Curves aren't downloaded until explicitly accessed via `retrieve()` or `get_contents()`
- First access fetches from API and saves to file
- Subsequent access reads from cached file

**Validation checks:**
- File exists and is readable
- Contains exactly 8760 numeric values
- Returns `WarningCollector` dict mapping curve keys to errors

**Session vs Scenario:**
- Can work with either `Session` or `SavedScenario` objects
- Internally normalizes to `Session` to get ETEngine session ID

**Warning behavior:**
- Warnings auto-clear on each `update()` call to show only current issues
- Manual `.clear()` available for custom workflows (rarely needed)

---

## Sortables (Merit Order Rankings)

Sortables define the dispatch order for technologies in Merit/Fever calculations - which power plants dispatch first, the order of heat network dispatchables, etc.

### Quick Reference

| Operation | Method | Returns |
|-----------|--------|---------|
| Fetch | `scenario.sortables` | `Sortables` collection |
| View names | `sortables.names()` | List of sortable names |
| View as dict | `sortables.as_dict()` | Dict (API format) |
| View as DataFrame | `sortables.to_dataframe()` | DataFrame |
| Validate | `sortables.is_valid_update(data)` | Dict of warnings |
| Update | `scenario.update_sortables(data)` | Updated scenario |
| Remove | `scenario.remove_sortables(keys)` | Updated scenario |

### Fetching Sortables

```python
# Access sortables (auto-fetches on first access)
sortables = scenario.sortables

# List all sortable names
names = sortables.names()
# ['forecast_storage', 'heat_network_lt', 'heat_network_mt', 'heat_network_ht']

# List sortable types (may have duplicates for heat_network)
types = sortables.keys()
```

### Viewing Sortables

```python
# Export to dict (matches API format)
sortables_dict = sortables.as_dict()
# {
#     "forecast_storage": ["item1", "item2", "item3"],
#     "heat_network": {
#         "lt": ["dispatchable1", "dispatchable2"],
#         "mt": ["dispatchable3"],
#         "ht": []
#     }
# }

# Export to DataFrame
df = sortables.to_dataframe()
# Each column is a sortable, rows are the ordered items
print(df.head())
```

### Validating Updates

```python
# Pre-validation check (returns WarningCollector dict, doesn't modify state)
updates = {
    "forecast_storage": ["new_item_1", "new_item_2", "new_item_3"],
    "heat_network_lt": ["dispatchable_a", "dispatchable_b"]
}

warnings = sortables.is_valid_update(updates)

if len(warnings) == 0:
    # Safe to update - won't trigger warnings
    sortables.update(updates)
else:
    for name, warning in warnings.items():
        print(f"Validation error in {name}: {warning}")
```

### Setting and Altering Sortables

```python
# Update locally WITH validation and warning display
# Invalid orders are rejected, warnings are shown immediately
sortables.update({
    "forecast_storage": ["item1", "item2", "item3"]
})
# No warnings (valid update)

sortables.update({
    "nonexistent_sortable": ["item1"]
})
#  nonexistent_sortable: Sortable 'nonexistent_sortable' does not exist

sortables.update({
    "forecast_storage": ["item1", "item1", "item2"]  # Duplicate!
})
#  forecast_storage: Order contains duplicate items
# Value is NOT changed (keeps old value for data integrity)

# Update and upload to API (validates and raises exception if invalid)
scenario.update_sortables({
    "forecast_storage": ["item1", "item2", "item3"],
    "heat_network_lt": ["dispatchable1", "dispatchable2"]
})

# Update from DataFrame
df = pd.DataFrame({
    "forecast_storage": ["item1", "item2", "item3"],
    "heat_network_lt": ["dispatchable1", "dispatchable2", None]
})
scenario.set_sortables_from_dataframe(df)

# Remove sortables (revert to defaults)
scenario.remove_sortables(["forecast_storage"])
```

### Quirks and Special Behaviors

**Heat network special case:**
- Heat networks always require a subtype: `lt`, `mt`, or `ht` (low/medium/high temperature)
- Updates use nested dict format:
  ```python
  {
      "heat_network": {
          "lt": ["item1", "item2"],
          "mt": ["item3"],
          "ht": []
      }
  }
  ```
- The collection creates separate `Sortable` objects for each subtype
- Names include subtype: `"heat_network_lt"`, `"heat_network_mt"`, `"heat_network_ht"`

**Two payload formats:**
- **Flat list**: Simple types like `{"forecast_storage": ["item1", "item2"]}`
- **Nested dict**: Heat networks `{"heat_network": {"lt": [...], "mt": [...]}}`

**Validation rules:**
- No duplicate items within a single sortable order
- Maximum 17 items per sortable
- `heat_network` type must always specify subtype
- No duplicate sortable names in the collection

**Name vs Type:**
- `type`: Base type (e.g., `"heat_network"`)
- `name()`: Display name including subtype (e.g., `"heat_network_lt"`)

**DataFrame handling:**
- Exports with columns named by sortable name (including `_lt`, `_mt`, `_ht` suffixes)
- Imports detect `"heat_network_*"` pattern to recreate subtype structure
- Shorter lists are padded with `None` to match DataFrame row count

**Warning behavior:**
- Warnings auto-clear on each `update()` call to show only current issues
- Manual `.clear()` available for custom workflows (rarely needed)

---

## Common Patterns

### Validation and Warnings

All submodels use a consistent validation pattern:

**Local updates (`.update()` methods):**
- Automatically validate using internal checks
- Invalid values are **rejected** (not applied) for data integrity
- Warnings are **auto-displayed** immediately in console/notebook
- Non-existent keys/names trigger warnings

```python
# Local updates auto-validate and display warnings
inputs.update({"investment_costs_co2_ccs": -50.0})
#  investment_costs_co2_ccs: Value -50.0 should be between 0.0 and 500.0
# Value NOT changed (keeps old value)

sortables.update({"nonexistent": ["item1"]})
#  nonexistent: Sortable 'nonexistent' does not exist
```

**Pre-validation (`.is_valid_update()` methods):**
- Check validity **without applying** changes or side effects
- Returns `Dict[str, WarningCollector]` mapping keys to warnings
- Use before local updates if you want to handle errors programmatically

```python
# Pre-validation check
warnings = inputs.is_valid_update({"key": value})

if len(warnings) == 0:
    inputs.update({"key": value})  # Won't trigger warnings
else:
    for key, warning in warnings.items():
        print(f"Error in {key}: {warning}")
```

**API updates (`.update_user_values()`, `.update_sortables()`, etc.):**
- Validate first, then raise `ScenarioError` if invalid
- No silent failures - exceptions ensure you know about problems

```python
# API updates validate and raise exceptions
scenario.update_user_values({"key": invalid_value})
# Raises: ScenarioError: Could not update user values: ["key: ['validation error']"]
```

**Auto-clear behavior:**

Warnings automatically clear at the start of each `update()` call to show only current issues:

```python
# First update shows its warnings
inputs.update({"key": -50.0})
# key: Value -50.0 out of bounds

# Second update clears previous warnings and shows new ones
inputs.update({"key": 25.0})
# No warnings - previous warning cleared, value is now valid

# Each update starts fresh
inputs.update({"another_key": bad_value})
# Only see warnings from this update, not previous ones
```

**Manual clearing (rarely needed):**

The auto-clear behavior handles most cases. Manual `.clear()` is only needed for custom workflows:

```python
# Manually clear if needed (uncommon)
inputs.warnings.clear()
```

### Working with Warnings

**Display warnings:**
```python
# Automatic display (happens during update() calls)
inputs.update({"key": bad_value})
# Warnings auto-displayed immediately

# Manual display
inputs.show_warnings()  # Print all warnings to console
```

**Check for warnings programmatically:**
```python
# Check if any warnings exist
if len(inputs.warnings) > 0:
    print(f"Found {len(inputs.warnings)} warnings")

# Check for warnings on specific field
if inputs.warnings.has_warnings("key_name"):
    field_warnings = inputs.warnings.get_by_field("key_name")
    for warning in field_warnings:
        print(f"{warning.severity}: {warning.message}")
```

**Clear warnings:**
```python
# Clear all warnings from collection
inputs.warnings.clear()

# Clear warnings from individual item (if needed)
input_obj = inputs["investment_costs_co2_ccs"]
input_obj.warnings.clear()
```

**Best practices:**
- Review warnings after batch operations before continuing
- Clear warnings after addressing issues to avoid confusion
- Don't rely on warnings for flow control - use `is_valid_update()` for that

### DataFrame Import/Export

All submodels support DataFrame serialization for Excel workflows:

```python
# Export
df = scenario.inputs.to_dataframe(columns="value")
df = scenario.sortables.to_dataframe()
# Note: Custom curves ARE DataFrames (8760 rows × N curve columns)

# Import
scenario.set_user_values_from_dataframe(inputs_df)
scenario.set_sortables_from_dataframe(sortables_df)
scenario.update_custom_curves(CustomCurves._from_dataframe(curves_df, scenario.session.id))
```

### Batch Updates

Each update method can handle multiple items in a single call:

```python
# Batch multiple inputs in one API call
scenario.update_user_values({
    "input_1": 100.0,
    "input_2": 200.0,
    "input_3": 300.0
})

# Batch multiple sortables in one call
# (Note: This makes one API call per sortable type)
scenario.update_sortables({
    "forecast_storage": ["item1", "item2"],
    "heat_network_lt": ["dispatchable1", "dispatchable2"]
})

# Update custom curves
scenario.update_custom_curves(curves)
```

**Note on `skip_upload` parameter:**

The `skip_upload=True` parameter is primarily used by the `Scenarios.from_excel()` workflow to control which Excel sheets get uploaded based on the `update_set` parameter. For typical scripts, use the default `skip_upload=False` to upload changes immediately.

### Lazy Loading

All submodels use property-based lazy loading:

```python
# First access triggers API fetch
inputs = scenario.inputs  # Fetches from API

# Subsequent access uses cached value
inputs = scenario.inputs  # Returns cached data

# Cache is invalidated on updates
scenario.update_user_values({"key": value})
inputs = scenario.inputs  # Fresh fetch after update
```
