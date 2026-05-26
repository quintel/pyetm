# Working with Excel

pyetm provides comprehensive Excel import/export functionality for managing scenario data. This allows you to work with scenarios in bulk via spreadhseets.

## Excel Input File Structure

pyetm uses a standardized Excel format with multiple sheets:

| Sheet Name | Contents | Optional? |
|------------|----------|-----------|
| `MAIN` | Scenario metadata (ID, title, area, end year, etc.) | |
| `EXPORT_CONFIG` | Export configuration for scenario(s) | |
| `USERS` | User access permissions for scenarios | optional |
| `INPUTS` | User values for each scenario | optional |
| `SORTABLES` | Technology ordering (merit order, heat network) | optional - flexible name |
| `CUSTOM_CURVES` | Custom curves for specific or multiple scenarios | optional - flexible name |

### MAIN Sheet Fields

The `MAIN` sheet contains scenario metadata with the following fields:

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| `short_name` | String | Short identifier for the scenario | `BASE` |
| `scenario_id` | Integer | Saved scenario ID (if loading existing) | `123456` |
| `session` | Boolean | If true, indicates you are working with sessions, for example scenario_id now refers to a session_id (defaults to false) | `TRUE` |
| `copy_from` | Integer | ID of scenario to copy from | `111111` |
| `title` | String | Scenario name | `My Scenario` |
| `area_code` | String | Dataset area code (also indicates start year) | `nl2023` |
| `end_year` | Integer | Future year for the scenario | `2050` |
| `private` | Boolean | Whether scenario is private | `true`, `false` |
| `custom_curves` | String | Name of custom curves sheet | `CUSTOM_CURVES` |
| `sortables` | String | Name of sortables sheet | `SORTABLES` |

### EXPORT_CONFIG Sheet Structure

| Field Name | Valid Values | Description | Example |
|------------|--------------|-------------|---------|
| `include_inputs` (or `inputs`) | `true`, `false`, `yes`, `no`, `1`, `0` | Export slider input values | `true` |
| `include_sortables` (or `sortables`) | `true`, `false`, `yes`, `no`, `1`, `0` | Export technology ordering | `true` |
| `include_custom_curves` (or `custom_curves`) | `true`, `false`, `yes`, `no`, `1`, `0` | Export custom price/demand curves | `true` |
| `include_gqueries` (or `gquery_results`, `gqueries`) | `true`, `false`, `yes`, `no`, `1`, `0` | Export query results | `true` |
| `hourly_curves` | `true`, `false`, OR comma-separated carriers | Controls hourly curve exports (see below) | `electricity, heat` |
| `annual_exports` | `true`, `false`, OR comma-separated types | Annual export types to include (see below) | `energy_flow, sankey` |
| `include_input_defaults` (or `defaults`) | `true`, `false` | Include default input values (not just user-set) | `false` |
| `include_input_min_max` (or `min_max`) | `true`, `false` | Include min/max bounds for inputs | `true` |

#### Hourly Curves Options

The `hourly_curves` field has three modes:

1. **Export all carriers** (set to `true`):
   ```
   | hourly_curves |
   |---------------|
   | true          |
   ```
   Exports curves for: `electricity`, `hydrogen`, `heat`, `methane`

2. **No hourly curves** (set to `false`):
   ```
   | hourly_curves |
   |---------------|
   | false         |
   ```

3. **Specific carriers or curves** (comma-separated list):
   ```
   | hourly_curves                    |
   |----------------------------------|
   | electricity, heat                |
   ```
   Or specify individual curve names:
   ```
   | hourly_curves                           |
   |-----------------------------------------|
   | merit_order, heat_network, hydrogen     |
   ```

**Available carrier types**:
- `electricity` - Maps to ETM: `merit_order``
- `heat` - Maps to ETM: `heat_network`
- `hydrogen` - Maps to ETM: `hydrogen`,
- `methane` - Maps to ETM: `network_gas`

**Available curve names** (can be specified individually):
`merit_order`, `electricity_price`, `heat_network`, `agriculture_heat`, `household_heat`, `buildings_heat`, `hydrogen`, `network_gas`, `residual_load`, `hydrogen_integral_cost`

!!! note "Validation"
    Invalid carrier or curve names will be filtered out with a warning. Only valid entries will be exported.

#### Annual Exports Options

The `annual_exports` field has three modes:

1. **Export all types** (set to `true`):
   ```
   | annual_exports |
   |----------------|
   | true           |
   ```
   Exports all 7 available types (see below)

2. **No annual exports** (set to `false`):
   ```
   | annual_exports |
   |----------------|
   | false          |
   ```

3. **Specific export types** (comma-separated list):
   ```
   | annual_exports                     |
   |------------------------------------|
   | energy_flow, sankey, costs_parameters |
   ```

**Available export types**:
- `production_parameters` - Production parameters for technologies
- `energy_flow` - Energy flow data between nodes
- `energy_flow_present` - Present-year energy flow data
- `molecule_flow` - Molecule flow data
- `sankey` - Sankey diagram data
- `storage_parameters` - Storage parameters for technologies
- `costs_parameters` - Cost parameters for technologies

!!! note "Validation"
    Invalid export type names will be filtered out with a warning. Only valid entries will be exported.

### From Excel

##### Import scenarios from an Excel file without uploading to the API:

```python
from pyetm import Scenarios

# Load/create scenarios locally (default: update=False)
scenarios = Scenarios.from_excel("../examples/inputs/my_scenarios.xlsx")
```

!!! warning "Update Default is False"
    By default, `update=False` to prevent accidental API modifications. Always explicitly set `update=True` or pass a list when you want to upload changes.

##### Import with the Update Parameter

The `update` parameter controls whether data is uploaded to the API:

```python
from pyetm import Scenarios

# Option 1: Load locally only (default)
scenarios = Scenarios.from_excel("scenarios.xlsx", update=False)
# Data is loaded into Python objects but NOT uploaded to ETM API

# Option 2: Upload all data types
scenarios = Scenarios.from_excel("scenarios.xlsx", update=True)
# Uploads: user_values, custom_curves, sortables, users to the ETM API

# Option 3: Upload specific data types only
scenarios = Scenarios.from_excel(
    "scenarios.xlsx",
    update=["user_values", "sortables"]
)
# Only uploads user_values and sortables, skips custom_curves and users
```

**Available update types:**
- `"user_values"` - Slider input values
- `"custom_curves"` - Custom price/demand curves
- `"sortables"` - Technology ordering
- `"users"` - User access permissions (Scenario only, not Session)


#### Import with ScenarioPacker

For advanced control, use `ScenarioPacker` directly:

```python
from pyetm.models import ScenarioPacker

# Load Excel into packer
packer = ScenarioPacker.from_excel("scenarios.xlsx")

# Access structured data
main_info = packer.main_info()  # DataFrame with scenario metadata
inputs_df = packer.inputs(fields="user")  # User values as DataFrame
sortables_df = packer.sortables()  # Sortables as DataFrame

# Access scenarios
scenarios = packer.scenarios
```

#### Export Configuration

Control what data is exported using `ExportConfig`:

```python
from pyetm import Scenario
from pyetm.models import ExportConfig

# Create a scenario
scenario = Scenario.create(
    title="Export Example",
    area_code="nl",
    end_year=2050
)

# Configure exports
config = ExportConfig(
    include_inputs=True,           # Include input values
    include_sortables=True,        # Include technology ordering
    include_custom_curves=False,   # Skip custom curves
    include_gqueries=True,         # Include query results
    include_users=True,            # Include user permissions
    include_input_defaults=False,  # Don't include default values
    include_input_min_max=True,    # Include min/max bounds
    hourly_curves=["electricity", "heat"],  # Hourly curves for these carriers
    include_annual_exports=["energy_flow", "sankey"]  # Annual exports
)

# Set configuration on scenario
scenario.set_export_config(config)

# Later retrieve the config
current_config = scenario.get_export_config()
```

#### ExportConfig Options

| Parameter | Type | Description |
|-----------|------|-------------|
| `include_inputs` | `bool` | Export slider input values |
| `include_sortables` | `bool` | Export technology ordering |
| `include_custom_curves` | `bool` | Export custom curves |
| `include_gqueries` | `bool` | Export query results |
| `include_users` | `bool` | Export user permissions |
| `include_input_defaults` | `bool` | Include default input values (not just user-set) |
| `include_input_min_max` | `bool` | Include min/max bounds for inputs |
| `hourly_curves` | `List[str]` | Hourly curves for carriers: `"electricity"`, `"heat"`, `"hydrogen"`, `"methane"` |
| `include_annual_exports` | `List[str]` | Annual exports: `"energy_flow"`, `"sankey"`, `"production_parameters"`, etc. |

## To Excel

### Basic Export

Export a single scenario to Excel:

```python
from pyetm import Scenario

# Load or create a scenario
scenario = Scenario.load(123456)

# Export with default settings
scenario.to_excel("my_scenario.xlsx")
```

### Export Multiple Scenarios

Export a collection of scenarios to a single file:

```python
from pyetm import Scenario, Scenarios

# Create multiple scenarios
scenario_1 = Scenario.load(111111)
scenario_2 = Scenario.load(222222)
scenario_3 = Scenario.load(333333)

# Create collection
scenarios = Scenarios(items=[scenario_1, scenario_2, scenario_3])

# Export all to one file
scenarios.to_excel("comparison.xlsx")
```

### Export with Options

Customize what data is exported:

```python
from pyetm import Scenario

scenario = Scenario.load(123456)

# Export specific data types and carriers
scenario.to_excel(
    "custom_export.xlsx",
    hourly_curves=["electricity", "heat"],   # Only electricity and heat hourly curves
    include_inputs=True,                     # Include inputs
    include_sortables=True,                  # Include sortables
    include_custom_curves=False,             # Skip custom curves
    include_gqueries=True,                   # Include query results
    include_hourly_curves=True,                         # Include hourly curves
    include_input_defaults=True,                        # Include default input values
    include_input_min_max=True,                         # Include min/max bounds
    include_users=False,                                # Skip user permissions
    include_annual_exports=["energy_flow", "sankey"]    # Include specific annual exports
)
```

### Export with ScenarioPacker

Use `ScenarioPacker` for advanced export control:

```python
from pyetm.models import ScenarioPacker

# Create packer and add scenarios
packer = ScenarioPacker()
packer.add(scenario_1, scenario_2, scenario_3)

# Add specific data types
packer.add_inputs(scenario_1, scenario_2)
packer.add_sortables(scenario_1)
packer.add_hourly_output_curves(scenario_2, carrier_type="electricity")
packer.add_annual_exports(scenario_3, exports=["energy_flow"])

# Export
packer.to_excel(
    "advanced_export.xlsx",
    hourly_curves=["electricity"],
    include_inputs=True,
    include_annual_exports=["energy_flow", "sankey"]
)
```
