# Exports and Queries

Query scenario results and export data in various formats.

## Querying Results

### Basic Queries

Query specific output values from your scenario:

```python
from pyetm import Scenario

scenario = Scenario.from_scenario_id(123456)

# Query single result
renewability = scenario.query_result("dashboard_renewability")
print(f"Renewable percentage: {renewability:.1f}%")

# Query multiple results
results = scenario.query_results({
    "dashboard_total_costs",
    "dashboard_renewability",
    "dashboard_co2_emissions",
})

for key, value in results.items():
    print(f"{key}: {value}")
```

### Common Queries

Frequently-used queries:

```python
# Dashboard metrics
dashboard_metrics = scenario.query_results({
    "dashboard_renewability",
    "dashboard_total_costs",
    "dashboard_co2_emissions",
    "dashboard_reduction_relative_to_1990",
    "dashboard_total_demand",
})

# Electricity metrics
electricity_metrics = scenario.query_results({
    "total_electricity_produced",
    "total_electricity_consumed",
    "electricity_price_per_kwh",
})
```

## Hourly Exports

### Electricity Curves

Export hourly electricity production and consumption:

```python
# Fetch hourly curves
curves = scenario.fetch_hourly_electricity_curves()

# curves is a pandas DataFrame with 8760 rows (hours in a year)
print(curves.head())

# Save to CSV
curves.to_csv("hourly_electricity.csv")
```

### Heat Curves

Export hourly heat curves:

```python
heat_curves = scenario.fetch_hourly_heat_curves()
heat_curves.to_csv("hourly_heat.csv")
```

## Annual Exports

Export annual data for specific carriers:

```python
# Export annual electricity data
annual_electric = scenario.export_annual_data(carrier="electricity")

# Export for multiple carriers
carriers = ["electricity", "natural_gas", "hydrogen"]
for carrier in carriers:
    data = scenario.export_annual_data(carrier=carrier)
    data.to_csv(f"annual_{carrier}.csv")
```

## Custom Curves

Upload custom demand or production curves:

```python
import pandas as pd

# Load custom curve (must be 8760 hourly values)
custom_curve = pd.read_csv("my_custom_curve.csv")

# Upload to scenario
scenario.upload_custom_curve(
    curve_type="household_electricity",
    values=custom_curve["value"].tolist(),
)
```

## Data Formats

### CSV Export

Export with custom formatting:

```python
from pyetm.config import AppConfig

# Configure CSV settings
config = AppConfig(
    csv_separator=";",  # Use semicolon
    decimal_separator=",",  # European decimal format
)

# Export will use these settings
curves.to_csv("output.csv", sep=config.csv_separator)
```

### Excel Export

Export to Excel with multiple sheets:

```python
with pd.ExcelWriter("scenario_data.xlsx") as writer:
    scenario.fetch_hourly_electricity_curves().to_excel(writer, sheet_name="Electricity")
    scenario.fetch_hourly_heat_curves().to_excel(writer, sheet_name="Heat")
```

## Next Steps

- [Sessions and Authentication](sessions.md) - Learn about managing saved scenarios
- [Advanced Usage](advanced.md) - Batch processing and automation
- [API Reference](../api/) - Complete API documentation
