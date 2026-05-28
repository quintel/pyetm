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
