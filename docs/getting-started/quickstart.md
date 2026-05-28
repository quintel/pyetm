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
- Copy an input template Excel file to `inputs/input.xlsx`

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

## Running Scenarios from Excel

The easiest way to work with scenarios is using the `pyetm run` command with Excel files. This is ideal for non-technical users.

### Step 1: Prepare Your Input File

After running `pyetm init`, you'll have an `inputs/input.xlsx` file. Open it in Excel and configure your scenarios (see [the Excel docs](../user-guide/excel.md) for more detail):

### Step 2: Run Your Scenarios

Execute your scenarios and generate results:

```bash
pyetm run inputs/input.xlsx
```

This will:
1. Load scenario definitions from the Excel file
2. Update scenarios on the ETM platform with your input values
3. Fetch all results (annual exports, hourly curves, etc.)
4. Export everything to `inputs/input_results.xlsx`

**Available options:**

- `--output PATH` or `-o PATH` - Custom output file path
- `--no-update` - Read-only mode (don't upload changes to ETM)
- `--log-level LEVEL` - Logging verbosity

**Examples:**

```bash
# Basic usage (updates scenarios, default output)
pyetm run inputs/input.xlsx

# Custom output location
pyetm run inputs/input.xlsx --output results/my_results.xlsx

# Read-only mode (fetch data without updating scenarios)
pyetm run inputs/input.xlsx --no-update

# Verbose logging for debugging
pyetm run inputs/input.xlsx --log-level DEBUG
```

### Step 3: Review Your Results

Open the output Excel file to see:

- All your input data (for reference)
- Annual export results (CO₂ emissions, costs, energy use, etc.)
- Hourly curve data (electricity demand, solar production, etc.)
- Any custom queries you configured

Each sheet contains columns for all your scenarios, making it easy to compare results.

## Your First Scenario (Python API)

Let's create and run a simple scenario with pyetm:

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

Now let's change some inputs:

```python
# Update input values on the scenario
scenario.update_user_values({
    "capacity_costs_energy_flexibility_flow_batteries_electricity": 22,
    "costs_buildings_ht_heat_delivery_system_costs_eur_per_connection": 50,
})

scenario.inputs.to_dataframe()
```

## Querying Results

You can query any output value from your scenario:

```python
# Add queries and execute them
scenario.add_queries(["dashboard_bio_footprint", "dashboard_biomass_final_demand", "dashboard_biomass_import_share", "dashboard_total_costs"])
scenario.execute_queries()

# Get results (returns DataFrame with MultiIndex)
results = scenario.results()
results
```

## Exporting Data

Export your scenario's hourly electricity curves (a dictionary). Access the keys and grab just the `merit_order.csv`:

```python
# Export hourly curves to CSV via the session
curves = scenario.get_hourly_curves(['electricity'])
curves.keys()
merit_order = curves['merit_order']
# Save to file
merit_order.to_csv("hourly_electricity.csv")
```

You can also export your whole scenario to excel:
```python
scenario.to_excel("scenario.xlsx")
```

## Working with Excel Files Programmatically

You can also work with Excel files in Python using the `ScenarioPacker` class:

```python
from pyetm import ScenarioPacker

# Load scenarios from Excel (with updates)
packer = ScenarioPacker.from_excel("inputs/input.xlsx", update=True)

# Export results
packer.to_excel("inputs/results.xlsx")

# Load in read-only mode (no updates to ETM)
packer_readonly = ScenarioPacker.from_excel("inputs/input.xlsx", update=False)
```

This gives you programmatic control over the same workflow the `pyetm run` command uses.

## Using Jupyter Notebooks

pyetm works great with Jupyter notebooks! After running `pyetm init`, you'll have an input template you can customize and use in notebooks.

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
4. Choose **Python (pyetm)** or your .venv

## Next Steps

Now that you've created your first scenario, explore more advanced features:

- [Configuration Guide](configuration.md) - Learn about environment setup and SSL configuration
- [Working with Scenarios](../user-guide/scenarios.md) - Deep dive into scenario management
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
