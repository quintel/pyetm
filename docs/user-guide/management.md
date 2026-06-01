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

## Managing Your Scenarios

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

## Managing Users

Control who can access your scenarios by managing user permissions. Both Sessions and Saved Scenarios support user management with role-based access control.

### User Roles

Three roles determine what users can do with a scenario:

| Role | Description | Permissions |
|------|-------------|-------------|
| `scenario_owner` | Full control | Can edit, share, and delete the scenario |
| `scenario_collaborator` | Can modify | Can edit scenario settings and inputs |
| `scenario_viewer` | Read-only | Can view but not modify the scenario |

!!! note
    Use `"remove"` as a special role value to revoke a user's access entirely.

### Listing Users

Check who has access to a scenario:

```python
from pyetm import Scenario

# Load a saved scenario
scenario = Scenario.load(123456)

# List all users with access
users = scenario.list_users()

for user in users:
    print(f"{user['user_email']}: {user['role']}")
```

For Sessions (temporary scenarios), the API works the same way:

```python
from pyetm import Scenario

# Create or load a session
session = Scenario.create(area_code="nl2023", end_year=2050)

# List users
users = session.list_users()
```

### Adding or Updating User Access

Grant access or change a user's role:

```python
from pyetm import Scenario

scenario = Scenario.load(123456)

# Add a new collaborator
scenario.update_users(
    email="colleague@example.com",
    role="scenario_collaborator"
)

# Promote a viewer to owner
scenario.update_users(
    email="colleague@example.com",
    role="scenario_owner"
)
```

### Removing User Access

Revoke a user's access by setting their role to `"remove"`:

```python
from pyetm import Scenario

scenario = Scenario.load(123456)

# Remove a user's access
scenario.update_users(
    email="former-colleague@example.com",
    role="remove"
)
```

### Batch User Updates

For multiple user changes, use `skip_upload=True` to defer API calls, then apply all changes at once:

```python
from pyetm import Scenario

scenario = Scenario.load(123456)

# Queue multiple user updates
scenario.update_users("user1@example.com", "scenario_viewer", skip_upload=True)
scenario.update_users("user2@example.com", "scenario_collaborator", skip_upload=True)
scenario.update_users("user3@example.com", "remove", skip_upload=True)

# Apply all changes in one API call
scenario.apply_pending_users()
```

!!! tip "Bulk User Management with Excel"
    For managing users across multiple scenarios, use the Excel integration. The `USERS` sheet lets you view and update user permissions in a grid format. See [Working with Excel](excel.md#users-sheet) for details.

## Deleting Saved Scenarios

### Deleting Individual Scenarios

Use the `delete()` method to remove a saved scenario:

```python
from pyetm import Scenario

# Load and delete a scenario
scenario = Scenario.load(123456)
scenario.delete()
```

### Bulk Deletion

For deleting multiple scenarios at once, use `Scenarios.delete_many()`:

```python
from pyetm import Scenarios

# Delete multiple saved scenarios by ID
result = Scenarios.delete_many([123, 456, 789])

# Check results
print(f"Successfully deleted: {result['successful']}")
print(f"Failed to delete: {result['failed']}")
```

The method continues processing even if individual deletions fail, returning a summary of successes and failures.

## Managing Collections

Collections (also called transition paths) allow you to group multiple scenarios together, optionally with interpolation for multi-year projections.

### Loading Collections

```python
from pyetm import Collection

# Load a single collection
collection = Collection.load(123)
print(f"Collection: {collection.title}")
print(f"Scenarios: {collection.saved_scenario_ids}")

# Load all user collections
collections = Collection.load_all()
for collection in collections:
    print(f"{collection.id}: {collection.title}")
```

### Creating Collections

```python
from pyetm import Collection

# Create a simple collection
collection = Collection.create(
    title="My Collection",
    saved_scenario_ids=[1, 2, 3],
)

# Create an interpolated collection
collection = Collection.create(
    title="2030-2050 Pathway",
    saved_scenario_ids=[1],  # Base scenario
    area_code="nl",
    end_year=2050,
    interpolation=True,
)
```

!!! info "Collection Validation"
    - Collections can include up to 6 scenarios (combined `scenario_ids` and `saved_scenario_ids`)
    - All scenarios in a collection must use the same ETM version

### Updating Collections

```python
# Update collection attributes
collection.update(
    title="Updated Title",
    saved_scenario_ids=[1, 2, 3, 4],  # Add more scenarios
)

# Soft-delete a collection (mark as discarded)
collection.update(discarded=True)
```

### Deleting Collections

```python
# Permanently delete a collection
collection.delete()
```

!!! warning "Collection Deletion"
    Collection deletion is irreversible. The scenarios within the collection are not deleted, only the collection itself.
