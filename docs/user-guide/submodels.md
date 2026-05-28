# Working with Submodels

This guide covers the four main submodels in pyetm: **Inputs**, **Queries**, **Custom Curves**, and **Sortables**. Each section shows the core operations you'll use in practice.

For detailed API signatures and parameters, see the [API Reference](../api/index.md).

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
