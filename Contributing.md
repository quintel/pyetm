# Contributing to pyetm

Thank you for considering contributing to **pyetm**
This document explains how to set up your development environment, follow coding
standards, run tests, report issues and submit changes.

---

## 1. Development Setup

We use **[Poetry](https://python-poetry.org/)** to manage dependencies and environments. But of course
you can use whichever setup you're comfortable with.

Clone the repository:

```bash
git clone https://github.com/quintel/pyetm.git
cd pyetm
```

Install dependencies (including dev tools):

```bash
poetry install --with dev
```

**Set up pre-commit hooks** (one-time setup to automatically run checks on commit/push):

```bash
poetry run pre-commit install
```

Run commands inside the Poetry environment:

```bash
poetry run pytest
poetry run pyetm
```

Or enter the shell:

```bash
poetry shell
```

---

## 2. Code Standards

- **Python version**: 3.12+ (enforced in `pyproject.toml`)
- **Linting & Formatting**: [Ruff](https://docs.astral.sh/ruff/) (enforced via pre-commit hooks)
- **Type Checking**: [mypy](https://mypy.readthedocs.io/) in strict mode (enforced via pre-commit hooks)
- **Tests**: We would like to maintain a high test coverage, so please test your contributions

**Pre-commit hooks** automatically run on every commit/push (after running `poetry run pre-commit install` during setup):
- **Ruff**: Linting and code formatting
- **mypy**: Strict type checking

Run checks manually:
```bash
# Run all pre-commit checks
poetry run pre-commit run --all-files

# Run individual tools
poetry run ruff check src/pyetm       # Linting
poetry run ruff format src/pyetm      # Formatting
poetry run mypy src/pyetm             # Type checking
poetry run pytest                      # Tests
```

---

## 3. Contributing Workflow

1. **Fork** the repository on GitHub.
2. **Create a new branch** for your feature or bugfix:
   ```bash
   git checkout -b feature/my-new-feature
   ```
3. **Write code and tests.**
4. **Run linting and tests** to ensure everything passes.
5. **Commit** using clear messages.
6. **Push** your branch and open a Pull Request.

---

## 4. Documentation

- User-facing docs live at [docs.energytransitionmodel.com](https://docs.energytransitionmodel.com/main/pyetm/introduction).
- Developer docs live here in the repo.
- Update **docstrings** when you add or modify functionality.

### How Documentation Auto-Generates

The API reference documentation is **automatically generated** from your source code using [MkDocs](https://www.mkdocs.org/) with the [mkdocstrings](https://mkdocstrings.github.io/) plugin. When you add, modify, or rename Python files in `src/pyetm/`, the documentation updates automatically on the next build.

**Example**: When `scenario_excel_service.py` was renamed to `src/pyetm/exporters/excel_exporter.py`, the API reference automatically updated to reflect the new module structure.

**Quick preview**:
```bash
poetry run mkdocs serve
# Visit http://127.0.0.1:8000/api/ to see your changes
```

### Type Hints and Documentation

**Type hints are documentation.** They appear in the generated API reference and help users understand function signatures, parameters, and return values.

The project uses **strict type checking** via [mypy](https://mypy.readthedocs.io/), enforced by pre-commit hooks:
- All functions and methods must have type hints
- Types are automatically extracted and displayed in the documentation
- mkdocstrings creates hyperlinks between types for easy navigation

**Before you commit**, pre-commit hooks will automatically run:
1. **Ruff** (linting and formatting)
2. **mypy** (type checking)

If type checking fails, your commit will be blocked until you fix the type issues.

**Manual type checking**:
```bash
# Check entire codebase
poetry run mypy src/pyetm

# Check specific file
poetry run mypy src/pyetm/models/scenario.py
```

### Verifying Documentation Changes Locally

Before pushing changes, verify that your documentation looks correct:

1. **Run pre-commit checks** (requires `poetry run pre-commit install` during initial setup):
   ```bash
   poetry run pre-commit run --all-files
   ```

2. **Preview documentation locally**:
   ```bash
   poetry run mkdocs serve
   ```
   Visit `http://127.0.0.1:8000` and navigate to **API Reference** to verify:
   - Your module appears in the navigation
   - Docstrings render correctly
   - Type hints are displayed properly
   - Code examples are formatted correctly

3. **Build documentation** (with strict mode to catch warnings):
   ```bash
   poetry run mkdocs build --strict
   ```

### CI/CD Deployment Process

Documentation is automatically built and deployed via GitHub Actions:

**Workflow** (`.github/workflows/docs.yml`):
1. **On every push/PR** to `main` or `version-2`:
   - Docs are built with `mkdocs build --strict`
   - Build artifacts are uploaded for review

2. **On push to `main` or `version-2`** (not PRs):
   - Docs are deployed to GitHub Pages
   - Published to docs.energytransitionmodel.com

**Where to check**:
- View deployment status: GitHub Actions tab in the repository
- Check deployment logs: Click on the "docs" workflow run
- Live site: [docs.energytransitionmodel.com/main/pyetm](https://docs.energytransitionmodel.com/main/pyetm/introduction)

### Troubleshooting Documentation Issues

| Problem | Solution |
|---------|----------|
| **Module not appearing in API reference** | Check that the file doesn't start with `_` (except `__init__.py`) and is in `src/pyetm/` |
| **Type information missing** | Verify all functions have type hints; run `poetry run mypy src/pyetm` to check |
| **Import errors during build** | Ensure the module imports work correctly; test with `python -c "from pyetm.your.module import YourClass"` |
| **Docstring not rendering** | Verify you're using [Google-style docstrings](https://google.github.io/styleguide/pyguide.html#38-comments-and-docstrings) |
| **Build fails with strict warnings** | Run `poetry run mkdocs build --strict` locally to see warnings; often caused by broken internal links |
| **Pre-commit blocks commit** | Fix mypy errors shown in output; type hints are required for all public functions |

**Example Google-style docstring**:
```python
def create_scenario(region: int, end_year: int, template_id: int | None = None) -> Scenario:
    """Create a new scenario in the ETM.

    Args:
        region: Area code for the scenario (e.g., 205 for Netherlands)
        end_year: Target year for the scenario (e.g., 2050)
        template_id: Optional session ID to use as template

    Returns:
        A new Scenario instance with the specified parameters

    Raises:
        APIError: If the API request fails
        ValidationError: If parameters are invalid
    """
    ...
```

### Technical Details: How It Works

For those who need to understand or extend the documentation system:

**Generation Pipeline**:
1. **`docs/gen_ref_pages.py`** scans `src/pyetm/` recursively
   - Skips files: `__init__.py`, `__main__.py`, files starting with `_`
   - Creates a `.md` stub for each module with `::: pyetm.module.path`

2. **mkdocstrings** processes `:::` directives during build
   - Extracts docstrings and type hints from source code
   - Renders them as formatted HTML with syntax highlighting
   - Creates hyperlinks between related types

3. **mkdocs-literate-nav** reads `docs/api/SUMMARY.md` (auto-generated)
   - Builds the navigation tree structure

4. **mkdocs** compiles everything into a static site in `site/`

**Key configuration** (in `mkdocs.yml`):
- `plugins`: `gen-files`, `mkdocstrings`, `literate-nav`, `section-index`
- `mkdocstrings` settings: Google-style docstrings, show source, cross-reference types
- Navigation structure mirrors the `src/pyetm/` directory structure

**Extending the system**:
- Modify `docs/gen_ref_pages.py` to change which files are included
- Edit `mkdocs.yml` to adjust mkdocstrings rendering options
- Add custom pages by creating `.md` files in `docs/`

---

## 5. Reporting Issues

- Check existing [issues](https://github.com/quintel/pyetm/issues) first.
- When reporting a bug, please include:
  - Steps to reproduce
  - Expected vs actual behavior
  - Relevant logs/tracebacks

---

## 6. Using pyetm as a Library (Integrating in Your Own Project)

You can depend on `pyetm` in your own repository to script scenario creation, batch updates, run queries, and export results (Excel/CSV) using the built–in packing utilities.

### Quick Start

Install (with Poetryor Pip):

```bash
poetry add pyetm
pip install pyetm
```

Set your API token (environment variable takes precedence over `config.env`):

```bash
export ETM_API_TOKEN="etm_<your_jwt_token_here>"
```

Basic usage to create + run a scenario and fetch results:

```python
from pyetm import Scenario

# Create a scenario from a template / region code (example numbers are illustrative)
# Note: template_id expects Session ID (ETEngine), not SavedScenario ID (MyETM)
scen = Scenario.new(region=205, end_year=2050, template_id=12345)

# Update some inputs
scen.update_inputs({
   "buildings_solar_pv_solar_radiation": 0.5,
   "transport_car_using_electricity_share": 0.42,
})

# Execute stored gqueries (if any were previously added)
scen.results()

# Export outputs (see ScenarioPacker below for multi-scenario)
print(scen.outputs.electricity_demand_total)
```

### ScenarioPacker for Batch Exports

`ScenarioPacker` lets you collect multiple `Scenario` objects and export all relevant tables (main metadata, inputs, gquery results, outputs, sortables, custom curves) to a single Excel workbook.

```python
from pyetm.models import Scenario
from pyetm.models import ScenarioPacker

scen_a = Scenario.new(region=205, end_year=2035)
scen_b = Scenario.new(region=205, end_year=2050)

packer = ScenarioPacker()
packer.add(scen_a, scen_b)
packer.add_queries(["total_co2_emissions", "electricity_demand"])

packer.to_excel("outputs/scenarios.xlsx", include_input_details=True)
```

You can also import back from Excel (round‑trip) using `ScenarioPacker.from_excel(path)` which will reconstruct scenarios + configuration references.

### Customising Export Behaviour

Attach an `ExportConfig` to a scenario to centrally influence what the packer includes:

```python
from pyetm.models.export_config import ExportConfig

scen_a.set_export_config(ExportConfig(include_inputs=True, include_exports=True))
```

When multiple scenarios have configs the first one encountered is used as a “global” baseline; explicit keyword arguments to `to_excel()` always win.

### Low-Level API Client Access

Most high-level actions use a default cached `BaseClient` internally. If you need to batch raw HTTP operations:

```python
from pyetm.clients.base_client import BaseClient, make_batch_requests, get_client

# Option 1: Use the default cached client (picks up ETM_API_TOKEN + base_url from env)
client = get_client()

# Option 2: Create a new client instance with custom config
client = BaseClient(token="etm_custom_token", base_url="https://beta.engine.energytransitionmodel.com/api/v3")

requests = [
   {"method": "GET", "url": "/api/v3/scenarios/"},
   {"method": "GET", "url": "/api/v3/areas"},
]

results = make_batch_requests(client, requests)
for r in results:
   if r.success:
      print(r.data)
   else:
      print("Error:", r.errors)
```

### Using Multiple Clients

You can create multiple client instances to interact with different environments or use different tokens:

```python
from pyetm import BaseClient, Scenario

# Production client
prod_client = BaseClient()  # Uses env vars

# Beta environment client
beta_client = BaseClient(
    base_url="https://beta.engine.energytransitionmodel.com/api/v3",
    token="etm_beta_..."
)

# Create scenarios on different environments
prod_scenario = Scenario.new(title="Prod Test", client=prod_client)
beta_scenario = Scenario.new(title="Beta Test", client=beta_client)
```

### Next Steps

For a deeper guide (advanced batching, async hints, Excel round‑trip nuances, testing strategies) see [the contributor docs](https://docs.energytransitionmodel.com/main/pyetm/contributor-docs).
