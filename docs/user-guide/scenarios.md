# Scenarios

Create, load, copy, and manage ETM scenarios programmatically.

## Session vs Scenario

pyetm has two scenario types with different purposes:

Users are encouraged to use scenarios for most purposes/

| Feature | Session | Scenario (SavedScenario) |
|---------|---------|--------------------------|
| **Level** | ETEngine | MyETM |
| **Title** | Optional | Required |
| **Sharing** | Public link only | User permissions (owner/collaborator/viewer) |
| **Use Case** | Quick calculations, batch processing | Long-term storage, collaboration |


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

**Minimal session:**
```python
from pyetm import Session

session = Session.new(area_code="nl2023", end_year=2050)
```

!!! tip "Minimal Data"
    You need at least an **area_code** and **end_year** to create a session, and also a **title** to create a scenario.

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

### From Template

```python
from pyetm import Session

# Copy from existing scenario/preset
session = Session.new(template_id=123456)
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
from pyetm import Scenario, Session

# Load saved scenario
scenario = Scenario.load(123456)

# Load session
session = Session.load(789012)
```

### Load Multiple

```python
from pyetm import Scenarios, Sessions

scenarios = Scenarios.load_many([111111, 222222, 333333])
sessions = Sessions.load_many([444444, 555555])
```

### With Custom Client

```python
from pyetm import Scenario
from pyetm.clients import BaseClient

beta_client = BaseClient(
    token="your_beta_token",
    base_url="https://beta-engine.energytransitionmodel.com"
)

scenario = Scenario.load(123456, client=beta_client)
```

## Properties and Metadata

### Accessing Properties

```python
from pyetm import Scenario

scenario = Scenario.load(123456)

# Key properties
print(f"ID: {scenario.id}, Session: {scenario.scenario_id}")
print(f"Title: {scenario.title}, Area: {scenario.area_code}")
print(f"End year: {scenario.end_year}, private?: {scenario.private}")
```

### Available Properties

| Property | Type | Description |
|----------|------|-------------|
| `id` | `int` | MyETM saved scenario ID (Scenario only) |
| `scenario_id` | `int` | ETEngine session ID |
| `title` | `str` | Scenario title (required for Scenario) |
| `area_code` | `str` | Geographic region (e.g., "nl", "de", "uk") |
| `start_year` | `int \| None` | Start year (typically 2019) |
| `end_year` | `int` | Target end year |
| `template_id` | `int \| None` | Preset scenario ID if copied from template |
| `keep_compatible` | `bool \| None` | Update with preset changes |
| `private` | `bool \| None` | Visibility (Scenario only) |
| `source` | `str \| None` | Source identifier |
| `url` | `str` | Full ETM URL |
| `version` | `str` | ETM version (from URL) |
| `created_at` | `datetime \| None` | Creation timestamp |
| `updated_at` | `datetime \| None` | Last update timestamp |
| `metadata` | `dict \| None` | Custom metadata |

### Update Metadata

```python
scenario = Scenario.load(123456)

# Update title/privacy
scenario.update(title="Updated Title", private=True)

# Custom metadata
scenario.update_metadata(
    author="Jane Doe",
    project="Renewable Energy Analysis"
)
```

### Set Identifier

```python
scenario.set_short_name("RES_2050_HIGH")
print(scenario.identifier())  # "RES_2050_HIGH"
```

## Copying Scenarios

### Copy Methods

| Method | Template Link |
|--------|---------------|
| `copy()` | No |
| `copy_with_preset()` | Yes |

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

print(f"Template ID: {preset_copy.template_id}")  # Points to original
```

## Interpolating Scenarios

Create intermediate-year scenarios by interpolating between 2 or more scenarios with different end years. Useful for generating multi-year pathway scenarios or filling gaps between existing scenarios.

### Basic Interpolation (Sessions)

Create interpolated sessions without saving to MyETM:

```python
from pyetm import Session, Sessions

# Create or load scenarios with different end years
session_2030 = Session.new(area_code="nl2023", end_year=2030)
session_2050 = Session.new(area_code="nl2023", end_year=2050)

# Interpolate to create intermediate years
interpolated = Sessions.interpolate(
    sessions=[session_2030, session_2050],
    target_years=[2035, 2040, 2045]
)

# Returns list of Session objects
print([s.end_year for s in interpolated])  # [2035, 2040, 2045]
```

### Interpolating Saved Scenarios

Create and save interpolated scenarios to MyETM:

```python
from pyetm import Scenario, Scenarios

# Load saved scenarios
scenario_2030 = Scenario.load(111111)
scenario_2050 = Scenario.load(222222)

# Interpolate and save to MyETM
interpolated = Scenarios.interpolate(
    scenarios=[scenario_2030, scenario_2050],
    target_years=[2035, 2040]
)

# Returns list of Scenario objects (saved to MyETM)
print([(s.title, s.end_year) for s in interpolated])
```

### Custom Titles

Specify custom titles for each interpolated scenario:

```python
interpolated = Scenarios.interpolate(
    scenarios=[scenario_2030, scenario_2050],
    target_years=[2035, 2040, 2045],
    titles=["Pathway 2035", "Pathway 2040", "Pathway 2045"]
)
```

### Multi-Scenario Interpolation

Interpolate between 3 or more scenarios:

```python
# Load multiple scenarios spanning different years
s_2030 = Scenario.load(111111)
s_2040 = Scenario.load(222222)
s_2050 = Scenario.load(333333)

# Interpolate between all of them
interpolated = Scenarios.interpolate(
    scenarios=[s_2030, s_2040, s_2050],
    target_years=[2035, 2045]
)
```

!!! info "Requirements"
    - All scenarios must have the **same area_code**
    - All scenarios must have **unique end_year** values
    - Target years must be **between** the earliest and latest scenario end years
    - Cannot interpolate **scaled scenarios**
    - For `Scenario.interpolate()`, scenarios must be saved (have an ID)


## Collections and Bulk Operations

### Creating Collections

```python
from pyetm import Scenarios, Scenario

scenario_1 = Scenario.load(111111)
scenario_2 = Scenario.load(222222)

scenarios = Scenarios(items=[scenario_1, scenario_2])
```

#### Main metadata

```python
scenario.combine.main_info()
```

### Collection Operations

| Operation | Method | Example |
|-----------|--------|---------|
| Add scenarios | `add(*scenarios)` | `scenarios.add(s1, s2)` |
| Extend | `extend(iterable)` | `scenarios.extend([s3, s4])` |
| Access by index | `[index]` | `scenarios[0]` |
| Iterate | `for ... in` | `for s in scenarios:` |
| Length | `len()` | `len(scenarios)` |
| Bulk export | `to_excel()` | `scenarios.to_excel("file.xlsx")` |
| Combined data | `.combine` | `packer = scenarios.combine` |
| Hourly curves | `get_hourly_curves()` | `scenarios.get_hourly_curves(["electricity"])` |
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

### Bulk Creation and Loading

| Method | Purpose | Example |
|--------|---------|---------|
| `create_many()` | Create multiple scenarios | `Scenarios.create_many(configs, area_code="nl2023")` |
| `load_many()` | Load multiple scenarios | `Scenarios.load_many([111, 222, 333])` |
| `Sessions.load_many()` | Load multiple sessions | `Sessions.load_many([444, 555])` |

**Example:**
```python
from pyetm import Scenarios

configs = [
    {"title": "Base", "user_values": {...}},
    {"title": "Policy A", "user_values": {...}},
]

scenarios = Scenarios.create_many(
    configs,
    area_code="nl2023",
    end_year=2050,
    raise_on_data_errors=False
)
```

### Extract Underlying Sessions

```python
scenarios = Scenarios(items=[Scenario.load(111), Session.load(222)])

# Get list of Session objects
sessions = scenarios.sessions
```

## Deleting Scenarios

!!! warning "Not Implemented"
    Scenario deletion is not currently supported in pyetm. Delete scenarios manually via the MyETM web interface or let sessions expire naturally.
