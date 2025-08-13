# pyetm

This package provides a set of tools for interaction with the Energy Transition Model's API.
Learn more about the Energy Transition Model [here](https://energytransitionmodel.com/).

The package is designed to be a modular tool that advanced users can incorporate into their scenario workflows.
The complete documentation is available [via the ETM documentation page](https://docs.energytransitionmodel.com/main/pyetm/introduction).

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

```bash
curl -sSL https://install.python-poetry.org | python3 -
```

After installation, ensure Poetry is available:
```bash
poetry --version
```


#### Install Dependencies

Navigate to the `pyetm` folder and install all dependencies:
```bash
cd pyetm
poetry install
```

This will:
- Create a virtual environment
- Install runtime dependencies
If you want development dependencies (testing, linting, etc.) then append the
"--with dev" flag to the install command.


#### Activating the Environment
You can either:
- Run commands inside Poetry’s environment:
  ```bash
  poetry run pytest
  poetry run pyetm
  ```
- Or activate the shell:
  ```bash
  eval $(poetry env activate)
  ```
  Then run commands normally:
  ```bash
  pytest
  pyetm
  ```


## Configuring Your Settings

You can configure your API token and base URL either with a **config.yml** file or environment variables.

### Option 1: `config.yml`
1. Duplicate the example file (`examples/config.example.yml`) and rename it to `config.yml`.
2. Edit `config.yml`:
   - **etm_api_token**: Your ETM API token (overridden by `$ETM_API_TOKEN` if set).
   - **base_url**: API base URL (overridden by `$BASE_URL` if set).
     Examples:
       - Production: `https://engine.energytransitionmodel.com/api/v3`
       - Beta: `https://beta.engine.energytransitionmodel.com/api/v3`
       - Stable engine snapshot: `https://2025-01.engine.energytransitionmodel.com/api/v3`
   - **local_engine_url**, **local_model_url**: URLs for a local ETM instance.
   - **proxy_servers**: (Optional) HTTP/HTTPS proxy URLs.
   - **csv_separator** and **decimal_separator**: Defaults are `,` and `.`.

Place `config.yml` in the project root (`pyetm/` folder).

### Option 2: Environment Variables
If you prefer, set these environment variables:
```bash
ETM_API_TOKEN=<your token>
BASE_URL=<api url>
LOCAL_ENGINE_URL=<optional local engine url>
LOCAL_MODEL_URL=<optional local model url>
```

---

## Cross-Platform Notes
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
    ```
