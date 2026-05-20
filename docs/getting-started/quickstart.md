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

- Ask which environment to use (production, beta, or local)
- Create a `.env` configuration file
- Copy example Jupyter notebooks and helper files

After initialization, you'll need to manually add your API token to the `.env` file if you want to create or modify scenarios (see [Adding Your API Token](#adding-your-api-token) below).

**Available options:**

- `--environment`: Target environment (`pro`, `beta`, or `local`)
- `--log-level`: Logging verbosity (`DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`)
- `--force`: Overwrite existing files without prompting

Example:

```bash
pyetm init --environment pro --log-level INFO
```

### Adding Your API Token

After running `pyetm init`, open the generated `.env` file and add your token:

1. Open `.env` in your text editor
2. Find the commented `# ETM_API_TOKEN=` line
3. Uncomment it by removing the `#`
4. Paste your full token after the `=` sign
5. Save the file

Your `.env` should look like this:

```env
ETM_API_TOKEN=etm_eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9...
ENVIRONMENT=pro
LOG_LEVEL=INFO
```

**Note:** API tokens are very long (1000+ characters). Make sure you paste the entire token.

## Your First Scenario

Let's create and run a simple scenario programmatically:

```python
from pyetm import Scenario

# Create a new scenario for the Netherlands
scenario = Scenario.create(
    title="My First Scenario",
    area_code="nl2023",
    end_year=2050
)

print(f"Created scenario {scenario.id}")
print(f"URL: {scenario.session.url}")
```

## Modifying Inputs

Now let's change some inputs to model a future with more solar energy:

```python
# Update input values on the scenario
scenario.update_user_values({
    "number_of_energy_power_solar_pv_solar_radiation": 50000000,
    "number_of_households_solar_pv_solar_radiation": 5000000,
})

# Add queries and execute them
scenario.add_queries(["dashboard_renewability", "dashboard_co2_emissions"])
scenario.execute_queries()

# Get results
results = scenario.results(columns=["future"])
print(f"Renewable percentage: {results.loc['dashboard_renewability', 'future']:.1f}%")
print(f"CO2 emissions: {results.loc['dashboard_co2_emissions', 'future']:.1f} Mton")
```

## Querying Results

You can query any output value from your scenario:

```python
# Add queries and execute them
scenario.add_queries([
    "dashboard_total_costs",
    "dashboard_renewability",
    "dashboard_co2_emissions",
])
scenario.execute_queries()

# Get results as a DataFrame
results = scenario.results(columns=["future"])

# Iterate through results
for key in results.index:
    print(f"{key}: {results.loc[key, 'future']}")
```

## Exporting Data

Export your scenario's hourly electricity curves:

```python
# Export hourly curves to CSV via the session
curves = scenario.session.hourly_output_curves.electricity_df

# Save to file
curves.to_csv("hourly_electricity.csv")
print("Exported hourly electricity curves")
```

## Saving Scenarios

To save your scenario permanently (requires authentication):

```python
from pyetm import Scenario, BaseClient

# Initialize authenticated client
client = BaseClient()

# Create and save a scenario (saved scenarios require a title)
scenario = Scenario.create(
    title="My First Scenario",
    area_code="nl2023",
    end_year=2050,
    client=client,
)

print(f"Saved scenario: {scenario.id}")
print(f"Session URL: {scenario.session.url}")
```

## Complete Example

Here's a complete example putting it all together:

```python
from pyetm import Scenario, BaseClient

# Initialize client (reads from .env)
client = BaseClient()

# Create a new scenario
scenario = Scenario.create(
    title="High Solar Scenario",
    area_code="nl2023",
    end_year=2050,
    client=client,
)

# Modify inputs
scenario.update_user_values({
    "number_of_energy_power_solar_pv_solar_radiation": 50000000,
    "households_solar_pv_solar_radiation_market_penetration": 75.0,
    "transport_car_using_electricity_share": 50.0,
})

# Add queries and execute them
scenario.add_queries([
    "dashboard_total_costs",
    "dashboard_renewability",
    "dashboard_co2_emissions",
    "dashboard_total_demand",
])
scenario.execute_queries()

# Get results
results = scenario.results(columns=["future"])

# Print results
print("\n=== Scenario Results ===")
for key in results.index:
    print(f"{key}: {results.loc[key, 'future']:.2f}")

# The scenario is already saved in MyETM
print(f"\nSaved scenario ID: {scenario.id}")
print(f"Session URL: {scenario.session.url}")
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
- [API Reference](../api/index.md) - Complete API documentation

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
