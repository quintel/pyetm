# Scenarios

Create, load, copy, and manage ETM scenarios programmatically.

## How a Scenario works

A `Scenario` is a saved scenario, like the ones you see in the ETM. It has a title, privacy,
users and a history of saved snapshots. Inputs, curves, queries and exports all work on its
current state.

Reading a public scenario needs no token. Creating, changing or listing scenarios needs an
`ETM_API_TOKEN`. See [Configuration](../getting-started/configuration.md).


## Creating Scenarios

### Basic Creation

**Minimal scenario:**
```python
from pyetm import Scenario

scenario = Scenario.new(
    title="Renewable Energy Transition",
    area_code="nl2023",
    end_year=2050
)
```

!!! tip "Minimal Data"
    You need a **title**, an **area_code** and an **end_year**. The scenario is created on the
    Version set by `ETM_VERSION`, which defaults to `latest`.

### With Initial Data

```python
from pyetm import Scenario

scenario = Scenario.new(
    title="High Solar Scenario",
    area_code="nl2023",
    end_year=2050,
    user_values={
        "battery_capacity_always_on_solar_pv_solar_radiation": 200,
        "capacity_of_energy_battery_solar_pv_solar_radiation": 50.0,
        "capacity_of_energy_heat_solar_ht_solar_thermal": 100,
        "flh_of_energy_power_solar_pv_solar_radiation": 1000
    },
    private=False
)
```

### From a Template

Templates are saved scenarios too, so start from one by copying it:

```python
from pyetm import Scenario

template = Scenario.load(123456)
scenario = template.copy_with_preset(title="Based on the template")
```

### Bulk Creation

```python
from pyetm import Scenarios

scenario_configs = [
    {
        "title": "Low Solar",
        "user_values": {"flh_of_energy_power_solar_pv_solar_radiation": 711}
    },
    {
        "title": "High Solar",
        "user_values": {"flh_of_energy_power_solar_pv_solar_radiation": 1110}
    }
]

scenarios = Scenarios.create_many(
    scenario_configs,
    area_code="nl2023",
    end_year=2050
)
```

## Loading Scenarios

### Load Single

```python
from pyetm import Scenario

scenario = Scenario.load(123456)
```

A scenario on a Version other than `ETM_VERSION` loads with a warning, shown once per Version.

### Load Multiple

```python
from pyetm import Scenarios

scenarios = Scenarios.load_many([111111, 222222, 333333])

# Every scenario you can access
mine = Scenarios.load_all()
```

### With Custom Client

```python
from pyetm import Scenario
from pyetm.clients import BaseClient

beta_client = BaseClient(
    token="your_beta_token",
    base_url="https://beta.my.energytransitionmodel.com/api/v4"
)

scenario = Scenario.load(123456, client=beta_client)
```

## Properties and Metadata

### Accessing Properties

```python
from pyetm import Scenario

scenario = Scenario.load(123456)

# Key properties
print(f"ID: {scenario.id}, Version: {scenario.version}")
print(f"Title: {scenario.title}, Area: {scenario.area_code}")
print(f"End year: {scenario.end_year}, private?: {scenario.private}")
```

### Available Properties

| Property | Type | Description |
|----------|------|-------------|
| `id` | `int` | The saved scenario's ID |
| `title` | `str` | Scenario title |
| `description` | `str \| None` | Scenario description |
| `area_code` | `str` | Geographic region (e.g., "nl2023", "de") |
| `end_year` | `int` | Target end year |
| `version` | `str` | The Version the scenario is on, e.g. "latest" |
| `private` | `bool \| None` | Visibility |
| `scenario_id` | `int \| None` | The ID of its current state, for reference only |
| `discarded_at` | `datetime \| None` | When the scenario was discarded, if it was |
| `created_at` | `datetime \| None` | Creation timestamp |
| `updated_at` | `datetime \| None` | Last update timestamp |
| `start_year` | `int \| None` | Start year of the dataset |
| `template_id` | `int \| None` | What the current state was copied from, if it keeps a preset link |
| `keep_compatible` | `bool \| None` | Whether the engine keeps it compatible with model updates |
| `source` | `str \| None` | Source identifier |
| `url` | `str \| None` | The API URL of its current state |
| `metadata` | `dict` | Custom metadata |

pyetm fetches the last six the first time you read one.

### Update Metadata

```python
scenario = Scenario.load(123456)

# Update title, description or privacy
scenario.update(title="Updated Title", description="Second draft", private=True)

# Custom metadata
scenario.update_metadata(
    author="Jane Doe",
    project="Renewable Energy Analysis"
)
```

### Save a Snapshot

Changes land on the scenario as you make them. Saving keeps the current state in the
scenario's history, where you can view and restore it in the ETM. Later changes carry on
from there.

```python
scenario.update_user_values({"investment_costs_co2_ccs": 50.0})
scenario.save()

# Later changes apply after the saved snapshot
scenario.update_user_values({"investment_costs_co2_ccs": 60.0})
```

### Set Identifier

The short name is a local label. Workbooks and exports use it to name the scenario's columns.

```python
scenario.set_short_name("RES_2050_HIGH")
print(scenario.short_name)  # "RES_2050_HIGH"
```

## Copying Scenarios

### Copy Methods

Both create a new saved scenario owned by you. The title defaults to
"Copy of" the original's, and the privacy to the original's.

| Method | Preset link to the original |
|--------|-----------------------------|
| `copy()` | No |
| `copy_with_preset()` | Yes, so its edits read against the original |

### Independent Copy

```python
from pyetm import Scenario

original = Scenario.load(123456)
copy = original.copy()

print(f"Template link: {copy.template_id}")  # None
```

### Copy with Overrides

```python
original = Scenario.load(123456)

modified = original.copy(
    user_values={
        "flh_of_energy_power_solar_pv_solar_radiation": 1300,
    },
    title="Modified Scenario",
    private=True
)
```

### Copy with Preset Link

```python
original = Scenario.load(123456)

preset_copy = original.copy_with_preset()

print(f"Template ID: {preset_copy.template_id}")  # Points at the original
```

## Interpolating Scenarios

Create scenarios for earlier end years, with inputs interpolated from one source scenario.
Each new scenario is saved.

```python
from pyetm import Scenario

scenario_2050 = Scenario.load(222222)

interpolated = Scenario.interpolate(scenario_2050, 2030, 2040)

print([(s.title, s.end_year) for s in interpolated])
# [('... interpolated to 2030', 2030), ('... interpolated to 2040', 2040)]
```

### Custom Titles

```python
interpolated = Scenario.interpolate(
    scenario_2050, 2030, 2040,
    titles=["Pathway 2030", "Pathway 2040"]
)
```

!!! info "Requirements"
    - Interpolate from **one source** at a time. Passing several scenarios raises an error.
    - Each end year must be **before** the source's end year and after its start year.
    - Cannot interpolate **scaled scenarios**.


## Working with Several Scenarios

To group saved scenarios for the Collections app, see [Managing Collections](management.md).

### Grouping Several Scenarios

```python
from pyetm import Scenarios, Scenario

scenario_1 = Scenario.load(111111)
scenario_2 = Scenario.load(222222)

scenarios = Scenarios(items=[scenario_1, scenario_2])
```

#### Main metadata

```python
scenarios.combine.main_info()
```

### Operations on Several Scenarios

| Operation | Method | Example |
|-----------|--------|---------|
| Add scenarios | `add(*scenarios)` | `scenarios.add(s1, s2)` |
| Extend | `extend(iterable)` | `scenarios.extend([s3, s4])` |
| Access by index | `[index]` | `scenarios[0]` |
| Iterate | `for ... in` | `for s in scenarios:` |
| Length | `len()` | `len(scenarios)` |
| Bulk export | `to_excel()` | `scenarios.to_excel("file.xlsx")` |
| Combined data | `.combine` | `packer = scenarios.combine` |
| Hourly curves | `get_hourly_output_curves()` | `scenarios.get_hourly_output_curves("electricity")` |
| Annual exports | `get_annual_exports()` | `scenarios.get_annual_exports(["energy_flow"])` |

### Bulk Export

```python
scenarios = Scenarios.load_many([111111, 222222, 333333])

scenarios.to_excel(
    "comparison.xlsx",
    include_inputs=True,
    hourly_curves=["electricity"]
)
```

### Access Combined Data

```python
scenarios = Scenarios.load_many([111111, 222222])

# Via ScenarioPacker
packer = scenarios.combine
inputs_df = packer.inputs(columns="user")
results_df = packer.gquery_results()
```

### Bulk Creation, Loading, and Management

| Method | Purpose | Example |
|--------|---------|---------|
| `create_many()` | Create multiple scenarios | `Scenarios.create_many(configs, area_code="nl2023")` |
| `load_many()` | Load multiple scenarios | `Scenarios.load_many([111, 222, 333])` |
| `load_all()` | Load every scenario you can access | `Scenarios.load_all()` |
| `discard_many()` | Discard multiple saved scenarios | `Scenarios.discard_many([111, 222, 333])` |

**Example:**
```python
from pyetm import Scenarios

configs = [
    {"title": "Base", "user_values": {...}},
    {"title": "Policy A", "user_values": {...}},
    # Copy an existing saved scenario instead of starting from scratch
    {"title": "Policy B", "source": 123456},
]

scenarios = Scenarios.create_many(
    configs,
    area_code="nl2023",
    end_year=2050,
)
```

A scenario that cannot be created, or whose data cannot be sent, becomes a warning. The rest
of the batch carries on.

### Bulk Discard

Discard multiple saved scenarios at once (soft-delete, recoverable):

```python
from pyetm import Scenarios

# Discard multiple saved scenarios by ID
result = Scenarios.discard_many([123456, 789012, 345678])

# Check results
print(f"Successfully discarded: {result['successful']}")
print(f"Failed to discard: {result['failed']}")
```

!!! info "Soft-Delete and Recovery"
    Scenarios are marked as discarded and hidden from listings, but can be recovered in the ETM within 60 days. After 60 days, they are removed discarded scenarios permanently.

The method continues processing even if individual discards fail, returning a summary of successes and failures.
