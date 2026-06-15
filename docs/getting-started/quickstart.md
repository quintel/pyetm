# Quick Start

This guide will walk you through creating your first ETM scenario with pyetm in just a few minutes.

## Prerequisites

Before starting, ensure you have:

- **Python 3.12+** installed on your system
- **pyetm package** installed (see [Installation](installation.md))
- **An ETM API token** (optional - see below for what requires authentication)
- A dedicated folder for your pyetm project (you'll create this in the next step)

## Getting an API Token

An API token is **optional** depending on what you want to do:

**Without a token, you can:**
- Create new public sessions
- Load and modify public sessions
- Query public scenario data
- Export results from public sessions

**With a token, you can additionally:**
- Access your saved scenarios in MyETM
- List all scenarios you've created
- Work with private scenarios
- Manage scenario sharing and permissions

To get a token:

1. Visit [the ETM API authentication docs](https://docs.energytransitionmodel.com/api/authentication)
2. Follow the instructions to generate your personal token
3. Keep your token secure - treat it like a password!

## Initialize Your Project

First, create a project folder and set up your Python environment:

```bash
# Create and navigate to your project folder
mkdir pyetm_project
cd pyetm_project

# Create a virtual environment
python3 -m venv .venv

# Activate the virtual environment
source .venv/bin/activate  # On macOS/Linux
# .venv\Scripts\activate   # On Windows

# Install pyetm
pip install pyetm
```

Then run the initialization command from inside your project folder:

```bash
pyetm init
```

This command will:

- Ask which environment to use (production, beta, or local - [learn more](https://docs.energytransitionmodel.com/api/intro#environments))
- Create a `.env` configuration file in your project folder
- Create an `excel/` folder with a template Excel file (`input.xlsx`)

After initialization, you can optionally add your API token to the `.env` file if you need access to saved scenarios or private scenarios (see [Adding Your API Token](#adding-your-api-token) below).

**Available options:**

- `--environment`: Target environment (`pro`, `beta`, or `local`)
- `--log-level`: Logging verbosity (`DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`)
- `--force`: Overwrite existing files without prompting

**Comprehensive example:**

```bash
pyetm init --environment pro
```

### Adding Your API Token (Optional)

After running `pyetm init`, you'll find a `.env` file in your project folder. If you need to access saved scenarios or private scenarios, open this file in any text editor and add your token:

1. Locate the `.env` file in your project folder (same location where you ran `pyetm init`)
2. Open `.env` in your text editor
3. Find the commented `# ETM_API_TOKEN=` line
4. Uncomment it by removing the `#`
5. Paste your full token after the `=` sign
6. Save the file

Your `.env` should look like this:

```env
ETM_API_TOKEN=etm_eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9...
ENVIRONMENT=pro
LOG_LEVEL=INFO
```

**Note:** API tokens are very long (1000+ characters). Make sure you paste the entire token.

## Running Scenarios from Excel

**This section shows how to run scenarios using the terminal/command-line.** For Python scripting and Jupyter notebooks, see [Your First Scenario (Python API)](#your-first-scenario-python-api) below.

The easiest way to work with scenarios is using the `pyetm run` command with Excel files. This is ideal for non-technical users.

### Step 1: Prepare Your Input File

After running `pyetm init`, you'll have an `excel/input.xlsx` file. Open it in Excel and configure your scenarios (see [the Excel docs](../user-guide/excel.md) for more detail):

### Step 2: Run Your Scenarios

In your terminal (from your project folder), execute your scenarios and generate results:

```bash
pyetm run excel/input.xlsx
```

This will:
1. Load scenario definitions from the Excel file
2. Update scenarios on the ETM platform with your input values
3. Fetch all results (annual exports, hourly curves, etc.)
4. Export everything to `excel/input_results.xlsx` **(in the same folder as your input file)**

**Available options:**

- `--output PATH` or `-o PATH` - Custom output file path
- `--no-update` - Read-only mode (don't upload changes to ETM)
- `--log-level LEVEL` - Logging verbosity

**Examples:**

```bash
# Basic usage (updates scenarios, output goes to excel/input_results.xlsx)
pyetm run excel/input.xlsx

# Custom output location (creates results folder if needed)
pyetm run excel/input.xlsx --output results/my_results.xlsx

# Read-only mode (fetch data without updating scenarios)
pyetm run excel/input.xlsx --no-update

# Verbose logging for debugging
pyetm run excel/input.xlsx --log-level DEBUG
```

### Step 3: Review Your Results

Open the output Excel file to see:

- All your input data (for reference)
- Annual export results (CO₂ emissions, costs, energy use, etc.)
- Hourly curve data (electricity demand, solar production, etc.)
- Any custom queries you configured

Each sheet contains columns for all your scenarios, making it easy to compare results.

## Your First Scenario (Python API)

**This section is for Python scripting and Jupyter notebooks.** If you prefer working from the terminal with Excel files, see [Running Scenarios from Excel](#running-scenarios-from-excel) above.

The Python API gives you programmatic control over scenarios, making it ideal for:
- Automating scenario creation and analysis
- Running batch experiments
- Building custom analysis workflows
- Interactive exploration in Jupyter notebooks

To use this approach, create a Python script (`.py` file) or Jupyter notebook (`.ipynb` file) in your project folder. For Jupyter setup instructions, see [Using Jupyter Notebooks](#using-jupyter-notebooks) below.

Let's create and run a simple scenario with pyetm:

```python
from pyetm import Scenario

# Create a new scenario for the Netherlands
scenario = Scenario.new(
    title="My First Scenario",
    area_code="nl2023",
    end_year=2050
)

print(f"Created scenario {scenario.id}")
print(f"URL: {scenario.session.url}")
```

### Modifying Inputs

Now let's change some inputs:

```python
# Update input values on the scenario
scenario.update_user_values({
    "capacity_costs_energy_flexibility_flow_batteries_electricity": 22,
    "costs_buildings_ht_heat_delivery_system_costs_eur_per_connection": 50,
})

scenario.inputs.to_dataframe()
```

### Querying Results

You can query any output value from your scenario:

```python
# Add queries and execute them
scenario.add_queries(["dashboard_bio_footprint", "dashboard_biomass_final_demand", "dashboard_biomass_import_share", "dashboard_total_costs"])
scenario.execute_queries()

# Get results (returns DataFrame with MultiIndex)
results = scenario.results()
results
```

### Exporting Data

Export your scenario's hourly electricity curves (a dictionary). Access the keys and grab just the `electricity_profiles.csv`:

```python
# Export hourly curves to CSV via the session
curves = scenario.get_hourly_curves(['electricity'])
curves.keys()
electricity_profiles = curves['electricity_profiles']
# Save to file
electricity_profiles.to_csv("hourly_electricity.csv")
```

!!! note "Deprecated curve names"
    The old curve names (`merit_order`, `heat_network`, `hydrogen`, `network_gas`) are deprecated but still supported with warnings. Please update your code to use the new names (`electricity_profiles`, `heat_network_profiles`, `hydrogen_profiles`, `network_gas_profiles`).

You can also export your whole scenario to excel:
```python
scenario.to_excel("scenario.xlsx")
```

### Working with Excel Files Programmatically

You can also work with Excel files in Python using the `ScenarioPacker` class (this is what the `pyetm run` command uses internally):

```python
from pyetm import ScenarioPacker

# Load scenarios from Excel (with updates)
packer = ScenarioPacker.from_excel("excel/input.xlsx", update=True)

# Export results
packer.to_excel("excel/results.xlsx")

# Load in read-only mode (no updates to ETM)
packer_readonly = ScenarioPacker.from_excel("excel/input.xlsx", update=False)
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
