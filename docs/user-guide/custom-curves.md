## Custom Curves (Hourly Profiles)

Custom curves are 8760-hour time series that override default profiles for technologies like solar PV production, EV charging patterns, or heat demand.

### Quick Reference

| Operation | Method | Returns |
|-----------|--------|---------|
| Fetch all | `scenario.custom_curves` | `CustomCurves` collection |
| Fetch single | `scenario.custom_curve_series(key)` | pandas Series (8760 values) or None |
| Check attached | `curves.is_attached(key)` | Boolean |
| Iterate all | `scenario.custom_curves_series()` | Generator yielding Series objects |
| Validate | `curves.is_valid_update(data)` | Dict of warnings |
| Update locally | `curves.update(data)` | Auto-displays warnings |
| Update API | `scenario.update_custom_curves(curves)` | Updated scenario |
| Remove from API | `scenario.remove_custom_curves(keys)` | Deletes curves and clears cache |

### Fetching Custom Curves

```python
# Access custom curves collection
curves = scenario.custom_curves

# Get a single curve as pandas Series
solar_curve = scenario.custom_curve_series("weather/solar_pv_on_roof_households")

# Iterate through all attached curves
for series in scenario.custom_curves_series():
    if series is not None:
        print(f"{series.name}")
```

### Viewing Custom Curves

List all attached curve keys
```python
attached_keys = list(curves.attached_keys())
```

Check if a specific curve is attached
```
if curves.is_attached("weather/solar_pv_on_roof_households"):
    print("Solar curve attached")
```

Get curve contents (fetches from API if not cached), returns pandas Series with 8760 values
```
curve_data = curves.get_contents(scenario, "weather/solar_pv_on_roof_households")
```

### Invalidating Cache

Force fresh data from API by removing cached curve files
```python
# Remove a single curve's cache file
curve = curves._find("weather/solar_pv_on_roof_households")
if curve:
    curve.remove()  # Deletes cache file, next access will fetch from API

# Remove all cached curves for a scenario
for curve in curves.curves:
    curve.remove()
```

### Setting and Altering Custom Curves

Update curves locally WITH validation and warning display
```python
import pandas as pd
import numpy as np

curves = scenario.custom_curves

# Create a custom curve (8760 hourly values)
solar_curve = pd.Series(np.ones(8760) * 0.5)
curves.update({"weather/solar_pv_on_roof_households": solar_curve})
# No warnings (valid 8760-hour curve)

short_curve = pd.Series(np.ones(100))
curves.update({"weather/solar_pv_on_roof_households": short_curve})
#  weather/solar_pv_on_roof_households: Curve must have 8760 values, found 100
# Curve file NOT updated (keeps old data)

curves.update({"nonexistent_curve": solar_curve})
#  nonexistent_curve: Curve 'nonexistent_curve' is not attached to scenario
```

Create CustomCurves from DataFrame for API upload
```python
df = pd.DataFrame({
    "weather/solar_pv_on_roof_households": np.ones(8760) * 0.5,
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

### Removing Custom Curves

Remove custom curves from the ETM API and clear local cache files:

```python
# Remove a single curve
scenario.remove_custom_curves(["weather/solar_pv_on_roof_households"])

# Remove multiple curves
scenario.remove_custom_curves([
    "weather/solar_pv_on_roof_households",
    "weather/wind_offshore_baseline"
])

# Also accepts sets instead of lists
scenario.remove_custom_curves({"weather/solar_pv_on_roof_households"})
```

**Behavior:**
- Deletes curves from the ETM API via DELETE endpoint
- Automatically clears local cache files for removed curves
- Removes curves from the scenario's local collection
- Raises `ScenarioError` if API deletion fails
- No-op if passed empty list/set

**Use Cases:**
- Reverting to default slider settings for published scenarios
- Cleaning up test curves during development
- Switching between custom and default configurations

### Quirks and Special Behaviors

**8760-hour requirement:**
- Every curve must have exactly 8760 values (one per hour of the year)
- Validation will warn if length != 8760 but proceeds with available data
- API upload will fail if values are missing

**File-based storage:**
- Curves are saved to `/tmp/pyetm/{scenario_id}/{curve_key}.csv`
- Prevents memory issues when working with hundreds of scenarios
- Files use sanitized keys: `curve.key.replace('/', '-')`

**Lazy loading:**
- Curves aren't downloaded until explicitly accessed via `retrieve()` or `get_contents()`
- First access fetches from API and saves to file
- Subsequent access reads from cached file

**Cache invalidation:**
- Cache files are **not automatically invalidated** when:

  - Uploading curves via `update_custom_curves()`
  - Reloading scenarios with `Scenario.load()`
  - Session updates or API changes
- Cache persists indefinitely until:

  - Manual deletion via `curve.remove()`
  - System `/tmp` cleanup (OS-dependent)
- **Warning**: Cache files can become stale if curves are modified via the ETM web interface or other clients
- To force fresh data: call `curve.remove()` before accessing, or manually delete cache files

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
