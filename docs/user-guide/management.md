# Sessions and Authentication

Learn about authentication, sessions, and managing saved scenarios.

## Authentication

### API Tokens

To save scenarios or access private scenarios, you need an API token:

1. Visit [ETM API Authentication](https://docs.energytransitionmodel.com/api/authentication)
2. Generate your personal token
3. Configure pyetm with your token

### Configuration

Set up authentication using a `.env` file:

```env
ETM_API_TOKEN=etm_your.token.here
ENVIRONMENT=pro
```

Or programmatically:

```python
from pyetm import Client
from pyetm.config import AppConfig

config = AppConfig(etm_api_token="etm_your.token.here")
client = Client(config=config)
```

## Working with Clients

### Creating a Client

```python
from pyetm import Client

# From environment variables / .env file
client = Client.from_env()

# With explicit configuration
from pyetm.config import AppConfig
config = AppConfig(etm_api_token="etm_your.token.here")
client = Client(config=config)
```

### Using Clients with Scenarios

```python
# Create authenticated scenario
scenario = Scenario.create(
    area_code="nl",
    end_year=2050,
    client=client,
)

# Load private scenario
scenario = Scenario.from_scenario_id(123456, client=client)
```

## Public vs. Private Scenarios

### Public Scenarios

- Anyone can view
- No authentication required to load
- Cannot be modified by others

```python
# Create public scenario
scenario = Scenario.create(area_code="nl", end_year=2050)
saved = scenario.save(private=False, client=client)
```

### Private Scenarios

- Only you can view
- Requires authentication
- Full control over modifications

```python
# Create private scenario
scenario = Scenario.create(area_code="nl", end_year=2050, client=client)
saved = scenario.save(private=True)
```

## Saved Scenarios

### Saving

Save a scenario to your account:

```python
scenario = Scenario.create(area_code="nl", end_year=2050, client=client)
scenario.user_values = {"input_key": 100.0}

saved = scenario.save(
    title="My Scenario",
    description="Test scenario for 2050",
    private=False,
)

print(f"Saved scenario: {saved.url}")
```

### Updating

Update an existing saved scenario:

```python
# Load saved scenario
scenario = Scenario.from_scenario_id(123456, client=client)

# Modify
scenario.user_values = {"input_key": 200.0}

# Save changes (same ID)
scenario.save()
```

### Deleting

Delete a scenario from your account:

```python
scenario.delete(client=client)
```

## Managing Multiple Scenarios

### List Your Scenarios

```python
# Get all scenarios for authenticated user
scenarios = client.get_my_scenarios()

for scenario in scenarios:
    print(f"{scenario.id}: {scenario.title}")
```

### Batch Operations

Work with multiple scenarios efficiently:

```python
# Load multiple scenarios
scenario_ids = [123456, 123457, 123458]
scenarios = [
    Scenario.from_scenario_id(sid, client=client)
    for sid in scenario_ids
]

# Process all
results = []
for scenario in scenarios:
    results.append({
        "id": scenario.id,
        "renewability": scenario.renewable_percentage,
        "costs": scenario.total_costs,
    })
```

## Next Steps

- [Advanced Usage](advanced.md) - Batch processing and automation
- [API Reference: Client](../api/clients/base_client.md) - Complete Client documentation
- [Configuration Guide](../getting-started/configuration.md) - Environment setup
