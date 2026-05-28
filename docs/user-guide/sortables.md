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

Access sortables (auto-fetches on first access)
```python
sortables = scenario.sortables
```

List all sortable names
```python
names = sortables.names()
```

### Viewing Sortables

Export to dict (matches API format)
```python
sortables_dict = sortables.as_dict()
```
Export to DataFrame
```python
df = sortables.to_dataframe()
```
Each column is a sortable, rows are the ordered items

### Setting and Altering Sortables

Fetch sortables first to cache valid items
```python
sortables = scenario.sortables
```
Update locally with validation and warning display
Invalid orders are rejected, warnings are shown immediately
```python
sortables.update({
    "forecast_storage": ["households_flexibility_p2p_electricity", "transport_car_flexibility_p2p_electricity"]
})
# No warnings (valid update with items from current scenario)

sortables.update({
    "nonexistent_sortable": ["item1"]
})
#  [WARNING] Sortable 'nonexistent_sortable' does not exist

sortables.update({
    "forecast_storage": ["item1", "item1", "item2"]  # Duplicate!
})
# [WARNING] Value error, Order contains duplicate items: ['item1']
# [WARNING] Unknown items not in current sortable: ['item1', 'item2']. Fetch sortables first to see valid items.
# Value is NOT changed (keeps old value for data integrity)

sortables.update({
    "forecast_storage": ["unknown_tech"]  # Not in current scenario!
})
#  [WARNING] Unknown items not in current sortable: ['unknown_tech']. Fetch sortables first to see valid items.
```

Update and upload to API (validates and raises exception if invalid)
```python
scenario.update_sortables({
    "forecast_storage": ["households_flexibility_p2p_electricity", "transport_car_flexibility_p2p_electricity", "transport_bus_flexibility_p2p_electricity"],
    "heat_network_lt": ["energy_heat_burner_lt_hydrogen", "energy_heat_boiler_lt_electricity"]
})

# Update from DataFrame - all arrays must be of the same length
df = pd.DataFrame({
    "forecast_storage": ["households_flexibility_p2p_electricity", "transport_car_flexibility_p2p_electricity", "transport_bus_flexibility_p2p_electricity"],
    "heat_network_lt": ["energy_heat_burner_lt_hydrogen", "energy_heat_boiler_lt_electricity", None]
})
scenario.set_sortables_from_dataframe(df)
```


Remove sortables (revert to defaults)
```python
scenario.remove_sortables(["forecast_storage"])
```

### Validation Against API

When you fetch sortables from a scenario, pyetm caches the valid technology keys from the API response. This cache is used to validate future updates:

```python
# Fetch sortables - this populates the cache with valid items
sortables = scenario.sortables
# Cache now contains: {"forecast_storage": {"tech_a", "tech_b", "tech_c"}, ...}

# Valid update - items are in the cache
sortables.update({"forecast_storage": ["tech_a", "tech_b"]})
# ✓ No warnings

# Invalid update - items NOT in the cache
sortables.update({"forecast_storage": ["unknown_item"]})
# ✗ Warning: Unknown items not in current sortable: ['unknown_item']

# To see what items are valid, check the current order
print(sortables.as_dict()["forecast_storage"])
# ['tech_a', 'tech_b', 'tech_c']
```

**Best practice:** Always fetch sortables before updating to ensure validation against the current scenario's available technologies.

**Note:** The cache only contains items that exist in the current sortable order. If you need to add technologies not currently in the order, you may need to refer to the API documentation or ETM interface to find valid technology keys for your scenario.

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
- Items must exist in the current scenario (validated against cached valid items from API)

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
