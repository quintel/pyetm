# Advanced Usage

Advanced patterns and techniques for power users and automation workflows.

## Scenario Packing

Scenario packing allows you to create many scenario variations efficiently by specifying input ranges.

### Basic Packing

```python
from pyetm.models import ScenarioPacker

# Define input variations
inputs = {
    "number_of_energy_power_solar_pv_solar_radiation": [10000, 50000, 100000],
    "transport_car_using_electricity_share": [25.0, 50.0, 75.0],
}

# Create packer
packer = ScenarioPacker(
    base_scenario_id=123456,
    inputs=inputs,
)

# Generate all combinations (3 × 3 = 9 scenarios)
scenarios = packer.create_all_scenarios()
print(f"Created {len(scenarios)} scenarios")
```

### Collecting Results

Process results from packed scenarios:

```python
results = []

for scenario in scenarios:
    results.append({
        "solar_pv": scenario.user_values["number_of_energy_power_solar_pv_solar_radiation"],
        "ev_share": scenario.user_values["transport_car_using_electricity_share"],
        "renewability": scenario.renewable_percentage,
        "costs": scenario.total_costs,
    })

# Convert to DataFrame for analysis
import pandas as pd
df = pd.DataFrame(results)
print(df)
```

## Async Operations

For handling many scenarios concurrently:

```python
import asyncio
from pyetm import Scenario

async def process_scenario(scenario_id):
    scenario = await Scenario.from_scenario_id_async(scenario_id)
    return {
        "id": scenario.id,
        "renewability": scenario.renewable_percentage,
    }

async def main():
    scenario_ids = [123456, 123457, 123458, 123459, 123460]
    results = await asyncio.gather(*[
        process_scenario(sid) for sid in scenario_ids
    ])
    return results

# Run async operations
results = asyncio.run(main())
```

## Batch Processing

Process large numbers of scenarios with error handling:

```python
from pyetm import Scenario
from pyetm.exceptions import ScenarioError

def process_scenarios_batch(scenario_ids, client):
    results = []
    errors = []

    for sid in scenario_ids:
        try:
            scenario = Scenario.from_scenario_id(sid, client=client)
            results.append({
                "id": sid,
                "renewability": scenario.renewable_percentage,
                "costs": scenario.total_costs,
            })
        except ScenarioError as e:
            errors.append({"id": sid, "error": str(e)})

    return results, errors

# Process batch
results, errors = process_scenarios_batch([123456, 123457, 123458], client)

print(f"Processed: {len(results)}, Errors: {len(errors)}")
```

## Custom Client Configuration

Advanced client setup for special use cases:

```python
from pyetm import Client
from pyetm.config import AppConfig

# Custom configuration
config = AppConfig(
    etm_api_token="etm_token.here",
    environment="beta",
    ssl_verify=False,  # For development only!
    log_level="DEBUG",
)

client = Client(config=config)
```

## Integration with Data Analysis

### Pandas Integration

Export data for analysis:

```python
import pandas as pd
from pyetm import Scenario

scenarios = [Scenario.from_scenario_id(sid) for sid in [123456, 123457]]

# Collect data
data = []
for s in scenarios:
    data.append({
        "scenario_id": s.id,
        "renewability": s.renewable_percentage,
        "co2": s.co2_emissions,
        "costs": s.total_costs,
    })

df = pd.DataFrame(data)
df.to_csv("scenario_comparison.csv")
```

### Visualization

Visualize scenario results:

```python
import matplotlib.pyplot as plt

# Plot renewability vs costs
plt.scatter(df["renewability"], df["costs"])
plt.xlabel("Renewable Percentage (%)")
plt.ylabel("Total Costs (billion €)")
plt.title("Cost vs. Renewability Trade-off")
plt.savefig("cost_renewability.png")
```

## Next Steps

- [API Reference](../api/) - Complete API documentation
- [Contributing](../contributing/guide.md) - Help improve pyetm
