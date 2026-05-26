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
prod_scenario = Scenario.create(title="Prod Test", area_code="nl2023", end_year=2050, client=prod_client)
beta_scenario = Scenario.create(title="Beta Test", area_code="nl2023", end_year=2050, client=beta_client)
```

# Managing Your Scenarios

PyETM provides convenient methods to list and manage all your scenarios and saved scenarios.

## Listing ETEngine Sessions

Use `Sessions.load_all()` to fetch all ETEngine sessions (temporary scenarios) belonging to the authenticated user:

```python
from pyetm import Sessions

# Fetch all your ETEngine sessions - automatically fetches all pages
sessions = Sessions.load_all()

# Iterate through sessions
for session in sessions:
    print(f"Session {session.id}: {session.title} ({session.area_code}, {session.end_year})")
```

### Pagination Control

By default, `load_all()` fetches all pages automatically. To fetch a specific page:

```python
# Fetch only page 1
sessions = Sessions.load_all(page=1)

# Fetch only page 2 with custom page size
sessions = Sessions.load_all(page=2, per_page=50)

# Control batch size when fetching all pages
sessions = Sessions.load_all(per_page=100)  # Fetches all pages, 100 at a time
```

## Listing MyETM Saved Scenarios

Use `Scenarios.load_all()` to fetch all your saved scenarios from MyETM:

```python
from pyetm import Scenarios

# Fetch all your saved scenarios
scenarios = Scenarios.load_all()

# Iterate through saved scenarios
for scenario in scenarios:
    print(f"Saved Scenario {scenario.id}: {scenario.title}")
    print(f"  - Based on session: {scenario.session.id}")
    print(f"  - Area: {scenario.session.area_code}")
    print(f"  - Private: {scenario.private}")
```

## Bulk Operations

Once you've loaded your scenarios, you can perform bulk operations:

```python
from pyetm import Scenarios

# Load all saved scenarios
scenarios = Scenarios.load_all()

# Export all to Excel - if you have a lot of scenarios this could take a while!
scenarios.to_excel("my_scenarios.xlsx")
```

## Filtering and Selection

Filter scenarios after loading them:

```python
from pyetm import Sessions

# Load all sessions
sessions = Sessions.load_all()

# Filter by area
nl_sessions = [s for s in sessions if s.area_code == "nl2023"]

# Filter by end year
sessions_2050 = [s for s in sessions if s.end_year >= 2050]

# Find specific session
my_session = next((s for s in sessions if "experiment" in (s.title or "")), None)
```
