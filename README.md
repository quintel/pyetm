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

You can configure your API token and base URL either with a **config.env** file or environment variables. You can simply set an `environment` and the base URL will be inferred for you.

### Option 1: `config.env` (Recommended)
1. Copy the example file (`example.config.env`) and rename it to `config.env`.
2. Edit `config.env`:
   ```bash
   # Your ETM API token (required)
   ETM_API_TOKEN=your.token.here

   # Environment (default: pro)
   ENVIRONMENT=pro

   # Optional: Override base URL directly
   # BASE_URL=https://engine.energytransitionmodel.com/api/v3

   # Optional: Proxy settings
   # PROXY_SERVERS_HTTP=http://user:pass@proxy.example.com:8080
   # PROXY_SERVERS_HTTPS=http://user:pass@secureproxy.example.com:8080

   # CSV settings (optional)
   CSV_SEPARATOR=,
   DECIMAL_SEPARATOR=.
   ```

Place `config.env` in the project root (`pyetm/` folder).

**Environment Options:**
- `pro` (default): Production environment
- `beta`: Staging environment
- `local`: Local development environment
- `YYYY-MM`: Stable tagged environment (e.g., `2025-01`)

### Option 2: Environment Variables
If you prefer, set these environment variables directly:
```bash
ETM_API_TOKEN=<your token>
ENVIRONMENT=<pro|beta|local|YYYY-MM>
# or provide a direct override instead of ENVIRONMENT
BASE_URL=<api url>
```

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
