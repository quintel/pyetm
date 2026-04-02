<img width="3392" height="736" alt="PyETM Logo (16xRes)" src="https://github.com/user-attachments/assets/3570d78f-681f-4360-935e-906a95807f15" />

---

This package provides a set of tools for interaction with the Energy Transition Model's API.
Learn more about the Energy Transition Model [here](https://energytransitionmodel.com/).

The package is designed to be a modular tool that advanced users can incorporate into their scenario workflows.
More documentation is available [via the ETM documentation page](https://docs.energytransitionmodel.com/main/pyetm/introduction).

---

## Installation

You can install **pyetm** directly from PyPI:
```bash
pip install pyetm
```

Or clone from [our GitHub repository](https://github.com/quintel/pyetm) if you want the latest development version:
```bash
git clone https://github.com/quintel/pyetm.git
cd pyetm
```

---

## Quick Start

After installing pyetm, initialize your project with:

```bash
pyetm init
```

This interactive command will:
- Prompt you for your ETM API token
- Ask which environment you want to use (production, beta, or local)
- Create a `.env` configuration file
- Copy example Jupyter notebooks and helper files to your current directory

**Get your API token**: Visit the [ETM API authentication docs](https://docs.energytransitionmodel.com/api/authentication) to obtain your personal API token.

### Non-Interactive Setup

You can also provide options directly to skip the prompts:

```bash
pyetm init --token etm_your.token.here --environment pro --log-level INFO
```

Options:
- `--token`: Your ETM API token
- `--environment`: Target environment (`pro`, `beta`, or `local`)
- `--log-level`: Logging verbosity (`DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`)
- `--force`: Overwrite existing files without prompting

---

## Running Jupyter Notebooks (Beginner Friendly)

If you only want to open and run our Jupyter notebooks in VS Code without developing the package,
follow the beginner guide here: [Running notebooks](running_notebooks.md).

---

## Development Setup (Using Poetry)

We recommend using [Poetry](https://python-poetry.org/) to manage dependencies and virtual environments.
Poetry ensures all dependencies are installed in an isolated environment, keeping your system clean.

### Python
Make sure you have **Python 3.12** or later installed:
- **Windows**: [Download from python.org](https://www.python.org/downloads/windows/)
- **macOS**: Install via [Homebrew](https://brew.sh/)
  ```bash
  brew install python@3.12
  ```
- **Linux**: Use your package manager or install from source.

Check your version:
```bash
python3 --version
```

---

### Poetry
Follow the [official instructions](https://python-poetry.org/docs/#installation):

#### For Mac with brew
```bash
brew install poetry
```

#### For other systems
Make sure pipx is installed, otherwise use:
```bash
brew install pipx
```

Then:
```bash
pipx install poetry
```

#### Finally
After installation, ensure Poetry is available:
```bash
poetry --version
```

#### Install Dependencies

Navigate to the `pyetm` folder and install all dependencies:
```bash
poetry install
```

This will:
- Create a virtual environment
- Install runtime dependencies
If you want development dependencies (testing, linting, etc.) then append the
"--with dev" flag to the install command.


#### How to use the environment:
You can either:
- Run commands inside Poetry's environment:
  ```bash
  poetry run pytest
  poetry run pyetm
  ```
- Or activate the shell:
  ```bash
  eval $(poetry env activate)
  ```
  Then run you can commands normally (e.g.):
  ```bash
  pytest
  ```


## Configuring Your Settings

You can configure your API token and base URL using environment variables or a `.env` file. You can simply set an `environment` and the base URL will be inferred for you.

### Option 1: Using `pyetm init` (Easiest)
Run the initialization command to interactively set up your configuration:
```bash
pyetm init
```
This will create a `.env` file with your settings. See the "Quick Start" section above for more details.

### Option 2: Manual `.env` File
1. Copy the example file (`.env.example`) and rename it to `.env`:
   ```bash
   cp .env.example .env
   ```
2. Edit `.env` with your settings:
   ```bash
   # Your ETM API token (required)
   ETM_API_TOKEN=your.token.here

   # Environment (default: pro)
   ENVIRONMENT=pro

   # Optional: Override base URL directly
   # BASE_URL=https://engine.energytransitionmodel.com/api/v3

   # Optional: Proxy settings
   # PROXY_SERVERS_HTTP=http://proxy.example.com:8080
   # PROXY_SERVERS_HTTPS=http://secureproxy.example.com:8080

   # CSV settings (optional)
   CSV_SEPARATOR=,
   DECIMAL_SEPARATOR=.
   ```

**For development**: Place `.env` in the repository root (`pyetm/` folder).

**For installed package**: Place `.env` in your project's working directory (where you run your Python scripts).

**Environment Options:**
- `pro` (default): Production environment
- `beta`: Staging environment
- `local`: Local development environment
- `YYYY-MM`: Stable tagged environment (e.g., `2025-01`)

### Option 3: Environment Variables
Set these environment variables directly in your shell:
```bash
ETM_API_TOKEN=<your token>
ENVIRONMENT=<pro|beta|local|YYYY-MM>
# or provide a direct override instead of ENVIRONMENT
BASE_URL=<api url>
```

**Note**: Environment variables take precedence over values in `.env` files.

---

### Temporary File Storage

The package stores temporary files (cached output curves and custom curves) in your system's temporary directory:
- **macOS/Linux**: `/tmp/pyetm/`
- **Windows**: `%TEMP%\pyetm\`

Files are organized by scenario ID to enable efficient caching across requests.

---

### Running Examples

The example notebooks in the `examples/` directory are designed for development and **require the repository to be cloned**. They use repository-relative paths and sample input files that are not included when installing via pip.

To run the examples:
1. Clone the repository:
   ```bash
   git clone https://github.com/quintel/pyetm.git
   cd pyetm
   ```
2. Install with development dependencies:
   ```bash
   poetry install --with dev
   ```
3. Configure your settings (see "Configuring Your Settings" above)
4. Open the notebooks in Jupyter or VS Code

---

### Notes
- **Windows**:
  - Use `py` instead of `python3` if `python3` is not recognized.
  - In PowerShell, set environment variables with:
    ```powershell
    $env:ETM_API_TOKEN="your-token"
    ```
- **macOS/Linux**:
  - Use `python3` in commands.
  - Set environment variables with:
    ```bash
    export ETM_API_TOKEN="your-token"
    export ENVIRONMENT=beta
    ```


## Contributing
See our [Contributing Guide](Contributing.md) for details.
