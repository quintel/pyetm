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

# Managing your scenarios

### List Your Scenarios

!!! warning "Not Implemented"
    Scenario management is not yet supported in pyetm. Manage scenarios manually via the MyETM web interface or let sessions expire naturally.
