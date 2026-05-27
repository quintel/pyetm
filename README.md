<img style="max-width:100%;height:auto;" alt="PyETM Logo (16xRes)" src="https://github.com/user-attachments/assets/3570d78f-681f-4360-935e-906a95807f15" />

---

This package provides a set of tools for interaction with the Energy Transition Model's API.
Learn more about the Energy Transition Model [here](https://energytransitionmodel.com/).

The package is designed to be a modular tool that advanced users can incorporate into their scenario workflows.
More documentation is available [via the docs](https://quintel.github.io/pyetm/).

---

## Installation

**Note: If you are not familiar working with python packages, please refer to the quick start guide first.**

Install **pyetm** from PyPI:
```bash
pip install pyetm
```

**Requirements**: Python 3.12 or later

Check your Python version:
```bash
python3 --version
```

If you need to install Python:
- **Windows**: [Download from python.org](https://www.python.org/downloads/windows/)
- **macOS**: Install via [Homebrew](https://brew.sh/): `brew install python@3.12`
- **Linux**: Use your package manager (e.g., `apt install python3`)

---

## Quick Start

### 1. Create a Virtual Environment

We recommend using a virtual environment to keep your project dependencies isolated:

```bash
# Create a new virtual environment
python3 -m venv .venv

# Activate it
# On macOS/Linux:
source .venv/bin/activate

# On Windows (PowerShell):
.venv\Scripts\Activate.ps1

# On Windows (Command Prompt):
.venv\Scripts\activate.bat
```

[More details on how python virtual environments work can be found here](https://docs.python.org/3/library/venv.html)

### 2. Install pyetm

With your virtual environment activated:
```bash
pip install pyetm
```

**Note: New versions of pyetm are consistently released. [Check here for the latest release](https://pypi.org/project/pyetm/#history)**

An example of how to install a specific release:
```bash
pip install pyetm==2.0.0
```

### 3. Initialize Your Project

Run the interactive setup command:
```bash
pyetm init
```

This will:
- Ask which environment you want to use (production, beta, or local)
- Create a `.env` configuration file
- Copy an input template Excel file to `inputs/input.xlsx`

After initialization, you'll need to manually add your API token to the `.env` file:

1. Open `.env` in your text editor
2. Find the commented `# ETM_API_TOKEN=` line
3. Uncomment it by removing the `#`
4. Paste your full token after the `=` sign (get your token [here](https://docs.energytransitionmodel.com/api/authentication))
5. Save the file

**Note:** API tokens are very long (1000+ characters). Make sure you paste the entire token.

**Command options**:
- `--environment`: Target environment (`pro`, `beta`, or `local`)
- `--log-level`: Logging verbosity (`DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`)
- `--force`: Overwrite existing files without prompting

Example:
```bash
pyetm init --environment pro --log-level INFO
```

### 4. Run Scenarios from Excel

The easiest way to work with scenarios is using Excel files:

```bash
# Edit inputs/input.xlsx with your scenario data, then run:
pyetm run inputs/input.xlsx
```

This will load your scenarios, update them on ETM, and export results to `inputs/input_results.xlsx`.

**Command options**:
- `--output PATH` or `-o PATH`: Custom output file location
- `--no-update`: Read-only mode (don't upload changes to ETM)
- `--log-level`: Logging verbosity

Example:
```bash
# Custom output location
pyetm run inputs/input.xlsx --output results/my_results.xlsx

# Read-only mode (fetch data without updating)
pyetm run inputs/input.xlsx --no-update
```

**For detailed Excel workflow instructions**, see the [Quick Start Guide](https://quintel.github.io/pyetm/getting-started/quickstart/#running-scenarios-from-excel) in the docs.

---

## Configuration

The `pyetm init` command creates a `.env` file with your settings. You can also configure manually:

### Environment Variables

Create a `.env` file in your project directory:

```bash
# Your ETM API token (required)
ETM_API_TOKEN=etm_your_token_here

# Environment (default: pro)
# Options: pro, beta, local, or stable tags like 2025-01
ENVIRONMENT=pro

# Logging level (default: INFO)
LOG_LEVEL=INFO

# CSV export settings (optional)
CSV_SEPARATOR=,
DECIMAL_SEPARATOR=.
```

**Environment Options:**
- `pro` (default): Production environment
- `beta`: Staging environment
- `local`: Local development environment
- `YYYY-MM`: Stable tagged environment (e.g., `2025-01`)

### Advanced Configuration

You can override the base URL directly if needed:
```bash
BASE_URL=https://engine.energytransitionmodel.com/api/v3
```

#### SSL/TLS Configuration

For corporate environments or custom certificate setups:

| Option | Default | Description |
|--------|---------|-------------|
| `SSL_VERIFY` | `true` | Verify SSL certificates. Set to `false` only for testing with self-signed certificates. **Never disable in production!** |
| `TRUST_ENV` | `false` | Enable system proxy environment variables (`HTTP_PROXY`, `HTTPS_PROXY`, `NO_PROXY`) |
| `SSL_CERT_PATH` | (empty) | Path to custom CA certificate bundle for corporate environments |

**Example for corporate CA:**
```bash
SSL_CERT_PATH=/path/to/corporate-ca-bundle.pem
TRUST_ENV=true
HTTP_PROXY=http://proxy.company.com:8080
```

For more advanced options, see [the full documentation](https://docs.energytransitionmodel.com/main/pyetm/introduction).

---

## Example Usage

After setup, you can use pyetm in your Python code. The main object you'll work with is the `Scenario`:

```python
from pyetm.models.scenario import Scenario

# Create a new scenario
scenario = Scenario.create({
    "area_code": "nl2023",
    "end_year": 2050,
    "title": "My Netherlands Scenario"
})

# Update inputs (sliders in the ETM)
scenario.update_user_values({
    "capacity_of_energy_power_solar_pv_solar_radiation": 15000,
    "capacity_of_energy_power_wind_turbine_inland": 8000
})

# Get results via gqueries
scenario.add_queries(['total_energy_demand', 'dashboard_total_costs'])
scenario.execute_queries()
results = scenario.results()
print(results)

# Load an existing scenario
existing = Scenario.load(saved_scenario_id=12345)
print(f"Loaded: {existing.title}")
```

### Working with Multiple Scenarios

```python
from pyetm.models.scenarios import Scenarios

# Load from Excel
scenarios = Scenarios.from_excel("inputs/my_scenarios.xlsx")

# Compare results across scenarios
comparison = scenarios.combine.gquery_results()
print(comparison)

# Export to Excel
scenarios.combine.to_excel("outputs/results.xlsx")
```

For more examples and tutorials, visit our [documentation](https://quintel.github.io/pyetm/examples/).

---

## Temporary File Storage

The package stores temporary files (cached curves and custom data) in your system's temp directory:
- **macOS/Linux**: `/tmp/pyetm/`
- **Windows**: `%TEMP%\pyetm\`

---

## Contributing

Interested in contributing to pyetm development? See our [Contributing Guide](Contributing.md) for details on:
- Setting up the development environment with Poetry
- Running tests
- Code style guidelines
- Submitting pull requests

---

## Getting Help

- **Documentation**: [docs.energytransitionmodel.com](https://docs.energytransitionmodel.com/main/pyetm/introduction)
- **Issues**: [GitHub Issues](https://github.com/quintel/pyetm/issues)
- **Repository**: [github.com/quintel/pyetm](https://github.com/quintel/pyetm)
