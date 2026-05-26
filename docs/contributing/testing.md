# Testing Guide

pyetm maintains high test coverage to ensure reliability. This guide explains our testing practices.

## Running Tests

### All Tests

```bash
poetry run pytest
```

### With Coverage

```bash
poetry run pytest --cov=pyetm --cov-report=term-missing
```

### Specific Tests

```bash
# Run one file
poetry run pytest tests/models/test_scenario.py

# Run one test
poetry run pytest tests/models/test_scenario.py::test_create_scenario

# Run tests matching pattern
poetry run pytest -k "scenario"
```

## Writing Tests

### Test Structure

```python
import pytest
from pyetm import Scenario

def test_scenario_creation():
    """Test creating a basic scenario."""
    scenario = Scenario.create(area_code="nl", end_year=2050)

    assert scenario.area_code == "nl"
    assert scenario.end_year == 2050
    assert scenario.id is not None
```

### Using Fixtures

```python
import pytest
from pyetm import Client

@pytest.fixture
def client():
    """Fixture for authenticated client."""
    return Client.from_env()

def test_with_client(client):
    """Test using the client fixture."""
    assert client is not None
```

### Mocking HTTP Requests

```python
import pytest
import requests_mock

def test_api_call():
    """Test API call with mocked response."""
    with requests_mock.Mocker() as m:
        m.get("https://api.example.com/data", json={"result": 42})

        response = requests.get("https://api.example.com/data")
        assert response.json()["result"] == 42
```

## Test Organization

```
tests/
├── conftest.py              # Shared fixtures
├── models/                  # Model tests
│   ├── test_scenario.py
│   ├── test_session.py
│   └── ...
├── services/                # Service tests
│   ├── test_scenario_runners.py
│   └── ...
└── test_cli.py              # CLI tests
```

## Best Practices

1. **Test one thing**: Each test should verify one behavior
2. **Clear names**: Use descriptive test names
3. **Arrange-Act-Assert**: Structure tests clearly
4. **Use fixtures**: Reuse common setup
5. **Mock external calls**: Don't rely on external APIs

## Next Steps

- [Development Setup](development.md) - Set up your environment
- [Contributing Guide](guide.md) - Contribution workflow
