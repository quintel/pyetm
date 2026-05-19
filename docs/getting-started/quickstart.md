# Quick Start

This guide will walk you through creating your first ETM scenario with pyetm in just a few minutes.

## Prerequisites

- Python 3.12+ installed
- pyetm package installed (see [Installation](installation.md))
- An ETM API token (optional, but required for saving scenarios)

## Getting an API Token

To interact with saved scenarios or create scenarios under your account, you'll need an API token:

1. Visit [the ETM API authentication docs](https://docs.energytransitionmodel.com/api/authentication)
2. Follow the instructions to generate your personal token
3. Keep your token secure - treat it like a password!

## Initialize Your Project

The easiest way to get started is using the interactive initialization:

```bash
pyetm init
```

This command will:

- Prompt you for your ETM API token
- Ask which environment to use (production, beta, or local)
- Create a `.env` configuration file
- Copy example Jupyter notebooks and helper files

### Non-Interactive Mode

You can also provide all options directly:

```bash
pyetm init --token etm_your.token.here --environment pro --log-level INFO
```

**Available options:**

- `--token`: Your ETM API token
- `--environment`: Target environment (`pro`, `beta`, or `local`)
- `--log-level`: Logging verbosity (`DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`)
- `--force`: Overwrite existing files without prompting

## Your First Scenario

Let's create and run a simple scenario programmatically:

```python
from pyetm import Scenario

# Create a new scenario for the Netherlands
scenario = Scenario.create(area_code="nl", end_year=2050)

print(f"Created scenario {scenario.id}")
print(f"URL: {scenario.url}")
```

## Modifying Inputs

Now let's change some inputs to model a future with more solar energy:

```python
# Set the number of solar PV panels
scenario.user_values = {
    "number_of_energy_power_solar_pv_solar_radiation": 50000000,
    "number_of_households_solar_pv_solar_radiation": 5000000,
}

# Fetch the results
print(f"Total electricity production: {scenario.total_electricity_production} PJ")
print(f"Renewable percentage: {scenario.renewable_percentage:.1f}%")
print(f"CO2 emissions: {scenario.co2_emissions:.1f} Mton")
```

## Querying Results

You can query any output value from your scenario:

```python
# Query specific outputs
results = scenario.query_results({
    "dashboard_total_costs",
    "dashboard_renewability",
    "dashboard_co2_emissions",
})

for key, value in results.items():
    print(f"{key}: {value}")
```

## Exporting Data

Export your scenario's hourly electricity curves:

```python
# Export hourly curves to CSV
curves = scenario.fetch_hourly_electricity_curves()

# Save to file
curves.to_csv("hourly_electricity.csv")
print("Exported hourly electricity curves")
```

## Saving Scenarios

To save your scenario permanently (requires authentication):

```python
from pyetm import Client

# Initialize authenticated client
client = Client.from_env()

# Create and save a scenario
scenario = Scenario.create(
    area_code="nl",
    end_year=2050,
    title="My First Scenario",
    client=client,
)

saved_scenario = scenario.save()
print(f"Saved scenario: {saved_scenario.url}")
```

## Complete Example

Here's a complete example putting it all together:

```python
from pyetm import Scenario, Client

# Initialize client (reads from .env)
client = Client.from_env()

# Create a scenario
scenario = Scenario.create(
    area_code="nl",
    end_year=2050,
    title="High Solar Scenario",
    client=client,
)

# Modify inputs
scenario.user_values = {
    "number_of_energy_power_solar_pv_solar_radiation": 50000000,
    "households_solar_pv_solar_radiation_market_penetration": 75.0,
    "transport_car_using_electricity_share": 50.0,
}

# Query results
results = scenario.query_results({
    "dashboard_total_costs",
    "dashboard_renewability",
    "dashboard_co2_emissions",
    "dashboard_total_demand",
})

# Print results
print("\n=== Scenario Results ===")
for key, value in results.items():
    print(f"{key}: {value:.2f}")

# Save the scenario
saved = scenario.save()
print(f"\nSaved scenario: {saved.url}")
```

## Using Jupyter Notebooks

pyetm works great with Jupyter notebooks! After running `pyetm init`, you'll have example notebooks ready to go.

### Setting Up Jupyter

1. **Install Jupyter** in your virtual environment:
   ```bash
   pip install notebook ipykernel
   ```

2. **Create a Jupyter kernel**:
   ```bash
   python -m ipykernel install --user --name=pyetm-env --display-name "Python (pyetm)"
   ```

3. **Launch Jupyter**:
   ```bash
   jupyter notebook
   ```

4. **Select the kernel**: In your notebook, go to **Kernel → Change kernel → Python (pyetm)**

### Using VS Code

If you prefer VS Code:

1. Install the [Python](https://marketplace.visualstudio.com/items?itemName=ms-python.python) and [Jupyter](https://marketplace.visualstudio.com/items?itemName=ms-toolsai.jupyter) extensions
2. Open a `.ipynb` file
3. Click **Select Kernel** (top right)
4. Choose **Python (pyetm)**

## Next Steps

Now that you've created your first scenario, explore more advanced features:

- [Configuration Guide](configuration.md) - Learn about environment setup and SSL configuration
- [Working with Scenarios](../user-guide/scenarios.md) - Deep dive into scenario management
- [Managing Inputs](../user-guide/inputs.md) - Learn about input handling and validation
- [API Reference](../api/) - Complete API documentation

## Common Issues

### SSL Certificate Errors

If you encounter SSL certificate errors, see the [Configuration Guide](configuration.md#ssl-configuration) for solutions.

### Token Authentication

Make sure your `.env` file contains a valid token:

```env
ETM_API_TOKEN=etm_your.token.here
ETM_ENVIRONMENT=pro
```

### Import Errors

Ensure your virtual environment is activated and pyetm is installed:

```bash
pip list | grep pyetm
```
