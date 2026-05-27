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
| Validate | `scenario.inputs.is_valid_update(data)` | Dict of warnings |
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

```python
# Update locally (not uploaded to API yet)
inputs.update({"investment_costs_co2_ccs": 50.0})

# Update and upload to API immediately
scenario.update_user_values({
    "costs_coal": 50.0,
    "capacity_costs_energy_flexibility_flow_batteries_electricity": 100.0
})

# Remove user values (revert to defaults)
scenario.remove_user_values([
    "costs_coal",
    "capacity_costs_energy_flexibility_flow_batteries_electricity"
])

# Set from DataFrame (input keys as index)
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
- **Validation warnings**: Invalid updates create `WarningCollector` objects instead of raising exceptions
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
# Execute all added queries
scenario.execute_queries()

# Now results are available
```

### Viewing Results

```python
# Get results as DataFrame
df = scenario.results(columns="future")  # Future year only
df = scenario.results(columns="present")  # Present year only
df = scenario.results(columns=["present", "future"])  # Both years

print(df)
#                                      future
# total_co2_emissions                   123.4
# total_costs_of_electricity           5678.9
# dashboard_total_costs               12345.6
```

### Query Results Structure

Each query result is a dictionary:

```python
# Access the internal Gqueries object
queries = scenario._queries

# Get a single query result
result = queries.get("total_co2_emissions")
# {
#     "present": 150.0,
#     "future": 123.4,
#     "unit": "MT"
# }
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
- If some queries are invalid, valid queries still return results
- Invalid queries generate warnings
- The system automatically retries with valid queries only

**Curve vs scalar queries:**
- Most queries return scalar values (single numbers)
- Some queries return 8760-hour arrays (e.g., electricity price curves)

**Lazy execution:**
- Queries start with `None` values
- Only populated after `execute_queries()` is called
- Use `gqueries.is_ready()` to check if execution is complete

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
| Update | `scenario.update_custom_curves(curves)` | Updated scenario |

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

# Example: flat production profile
flat_curve = pd.Series(np.ones(8760) * 0.5, name="weather/solar_pv_profile_1")

# Create CustomCurves from DataFrame
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
updates = {
    "forecast_storage": ["new_item_1", "new_item_2", "new_item_3"],
    "heat_network_lt": ["dispatchable_a", "dispatchable_b"]
}

# Validate before updating
warnings = sortables.is_valid_update(updates)

if len(warnings) == 0:
    print("All updates valid!")
else:
    for name, warning in warnings.items():
        print(f"{name}: {warning}")
```

### Setting and Altering Sortables

```python
# Update locally
sortables.update({
    "forecast_storage": ["item1", "item2", "item3"]
})

# Update and upload to API
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

---

## Common Patterns

### Validation Before Updates

All submodels use a consistent validation pattern with `WarningCollector`:

```python
# For inputs
warnings = scenario.inputs.is_valid_update({"key": value})

# For sortables
warnings = scenario.sortables.is_valid_update({"type": [items]})

# Check results
if len(warnings) == 0:
    # Safe to proceed
    pass
else:
    for key, warning in warnings.items():
        print(f"Error in {key}: {warning}")
```

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
