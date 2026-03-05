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
- **Linting**: [Pylint](https://pylint.pycqa.org/) (configured to require a minimum score of 8.0).
- **Tests**: We would like to maintain a high test coverage, so please test your contributions

Lint before committing

```bash
poetry run pylint src/pyetm
poetry run pytest
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
scen = Scenario.create(region=205, end_year=2050, template_id=12345)

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

scen_a = Scenario.create(region=205, end_year=2035)
scen_b = Scenario.create(region=205, end_year=2050)

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

Most high-level actions go through the singleton `BaseClient` internally. If you need to batch raw HTTP operations:

```python
from pyetm.clients.base_client import BaseClient, make_batch_requests

client = BaseClient()  # picks up ETM_API_TOKEN + base_url

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

### Next Steps

For a deeper guide (advanced batching, async hints, Excel round‑trip nuances, testing strategies) see [the contributor docs](https://docs.energytransitionmodel.com/main/pyetm/contributor-docs).
