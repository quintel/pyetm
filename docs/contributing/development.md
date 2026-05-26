# Development Setup

This guide explains how to set up a development environment for pyetm.

## Prerequisites

- Python 3.12 or later
- [Poetry](https://python-poetry.org/) for dependency management
- Git

## Setup

### 1. Clone the Repository

```bash
git clone https://github.com/quintel/pyetm.git
cd pyetm
```

### 2. Install Poetry

If you don't have Poetry installed:

```bash
curl -sSL https://install.python-poetry.org | python3 -
```

### 3. Install Dependencies

```bash
poetry install
```

This installs all dependencies including development tools.

### 4. Activate Virtual Environment

```bash
poetry shell
```

### 5. Configure Environment

Create a `.env` file:

```bash
cp .env.example .env
```

Edit `.env` with your settings:

```env
ETM_API_TOKEN=etm_your.token.here
ENVIRONMENT=pro
LOG_LEVEL=DEBUG
```

## Development Workflow

### Running Tests

```bash
# Run all tests
poetry run pytest

# Run with coverage
poetry run pytest --cov=pyetm --cov-report=term-missing

# Run specific test file
poetry run pytest tests/models/test_scenario.py

# Run specific test
poetry run pytest tests/models/test_scenario.py::test_create_scenario
```

### Code Quality

#### Ruff (Linting & Formatting)

```bash
# Check for issues
poetry run ruff check .

# Fix issues automatically
poetry run ruff check --fix .

# Format code
poetry run ruff format .
```

#### mypy (Type Checking)

```bash
# Type check entire codebase
poetry run mypy src/pyetm

# Type check specific file
poetry run mypy src/pyetm/models/scenario.py
```

#### Pylint

```bash
# Run pylint (must score >= 8.0)
poetry run pylint src/pyetm
```

### Pre-commit Hooks

pyetm uses pre-commit hooks to ensure code quality:

```bash
# Install hooks
poetry run pre-commit install

# Run manually
poetry run pre-commit run --all-files
```

Hooks run automatically on commit and check:

- Code formatting (Ruff)
- Type hints (mypy)
- Trailing whitespace
- TOML validity
- Merge conflicts

### Documentation

#### Build Docs Locally

```bash
# Serve docs locally
poetry run mkdocs serve

# Open http://127.0.0.1:8000 in your browser
```

#### Build for Production

```bash
poetry run mkdocs build

# Output in site/ directory
```

## Project Structure

```
pyetm/
├── src/pyetm/          # Source code
│   ├── clients/        # HTTP client infrastructure
│   ├── config/         # Configuration management
│   ├── models/         # Domain models (Scenario, Session, etc.)
│   ├── services/       # Business logic and API services
│   └── utils/          # Utility functions
├── tests/              # Test suite
├── docs/               # Documentation source
├── examples/           # Example inputs
├── pyproject.toml      # Project configuration
└── mkdocs.yml          # Documentation configuration
```

## Code Standards

### Style Guidelines

- **Line length**: 100 characters maximum
- **Docstrings**: Google style for all public APIs
- **Type hints**: Required for all functions and methods
- **Imports**: Organized (stdlib, third-party, local)

### Docstring Example

```python
def create_scenario(area_code: str, end_year: int) -> Scenario:
    """Create a new ETM scenario.

    Args:
        area_code: Region code (e.g., "nl", "de").
        end_year: Target year for the scenario.

    Returns:
        The created Scenario instance.

    Raises:
        ScenarioError: If scenario creation fails.

    Example:
        >>> scenario = create_scenario("nl", 2050)
        >>> print(scenario.id)
        123456
    """
    ...
```

### Testing Standards

- **Coverage**: Maintain high test coverage (>80%)
- **Unit tests**: Test individual functions and methods
- **Integration tests**: Test API interactions with mocks
- **Fixtures**: Use pytest fixtures for common setup

## Common Tasks

### Adding a New Feature

1. Create feature branch: `git checkout -b feature/my-feature`
2. Write tests first (TDD)
3. Implement feature
4. Update documentation
5. Run tests and linting
6. Submit pull request

### Fixing a Bug

1. Create bug fix branch: `git checkout -b fix/bug-description`
2. Write failing test that reproduces bug
3. Fix the bug
4. Verify test passes
5. Submit pull request

### Updating Dependencies

```bash
# Update all dependencies
poetry update

# Update specific dependency
poetry update pydantic

# Add new dependency
poetry add package-name

# Add dev dependency
poetry add --group dev package-name
```

## Troubleshooting

### Poetry Installation Issues

If you have issues installing dependencies:

```bash
# Clear cache
poetry cache clear pypi --all

# Reinstall
rm poetry.lock
poetry install
```

### Test Failures

If tests fail locally:

```bash
# Ensure environment is clean
poetry env remove python3.12
poetry install

# Run tests with verbose output
poetry run pytest -vv
```

## Next Steps

- [Testing Guide](testing.md) - Learn about testing practices
- [Contributing Guide](guide.md) - Contribution workflow
- [API Reference](../api/index.md) - Code documentation
