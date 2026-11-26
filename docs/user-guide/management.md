# Authentication and Management

Learn about authentication, clients, and managing your saved scenarios.

## Authentication

### API Tokens

Reading a public scenario needs no token. To create, change or list scenarios, or to read
private ones, you need a personal token:

1. Visit [your tokens page](https://my.energytransitionmodel.com/identity/tokens)
2. Generate a personal token
3. Configure pyetm with your token

### Configuration

Set up authentication using a `.env` file:

```env
ETM_API_TOKEN=etm_your.token.here
```

See
[Configuration](../getting-started/configuration.md) for every setting.

## Working with Clients

By default pyetm builds one client from your environment variables or `.env` file. Pass your
own `BaseClient` to use another token.

### Using Multiple Clients

```python
from pyetm import BaseClient, Scenario

# Production client
prod_client = BaseClient()  # Uses env vars

# Beta environment client
beta_client = BaseClient(
    base_url="https://beta.my.energytransitionmodel.com/api/v4",
    token="etm_beta_..."
)

# Create scenarios on different environments
prod_scenario = Scenario.new(title="Prod Test", area_code="nl2023", end_year=2050, client=prod_client)
beta_scenario = Scenario.new(title="Beta Test", area_code="nl2023", end_year=2050, client=beta_client)
```

A scenario keeps the client it was created or loaded with, and uses it for every later call.

## Managing Your Scenarios

### Listing Your Saved Scenarios

Use `Scenarios.load_all()` to fetch every saved scenario you can access. It follows
every page for you:

```python
from pyetm import Scenarios

scenarios = Scenarios.load_all()

for scenario in scenarios:
    print(f"Scenario {scenario.id}: {scenario.title}")
    print(f"  - Area: {scenario.area_code}, end year: {scenario.end_year}")
    print(f"  - Version: {scenario.version}, private: {scenario.private}")
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
from pyetm import Scenarios

scenarios = Scenarios.load_all()

# Filter by area
nl_scenarios = [s for s in scenarios if s.area_code == "nl2023"]

# Filter by end year
scenarios_2050 = [s for s in scenarios if s.end_year >= 2050]

# Find a specific scenario
mine = next((s for s in scenarios if "experiment" in (s.title or "")), None)
```

## Discarding and Deleting

```python
from pyetm import Scenario

scenario = Scenario.load(123456)

# Hidden from listings, recoverable for 60 days
scenario.discard()

# Permanent, cannot be undone
scenario.delete()
```

## Managing Users

Control who can access your scenarios by managing their users.

### User Roles

Three roles determine what users can do with a scenario:

| Role | Description | Permissions |
|------|-------------|-------------|
| `scenario_owner` | Full control | Can edit and share the scenario |
| `scenario_collaborator` | Can modify | Can edit scenario settings and inputs |
| `scenario_viewer` | Read-only | Can view but not modify the scenario |

!!! note
    Roles can be given with or without their `scenario_` prefix, so `"viewer"` works too.
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

Each user comes back as a dict with `user_email`, a short `role` such as `"owner"`, and
`pending`, which is true for an invitation not yet accepted.

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

# Apply all queued changes
scenario.apply_pending_users()
```

!!! tip "Bulk User Management with Excel"
    For managing users across multiple scenarios, use the Excel integration. The `USERS` sheet lets you view and update user permissions in a grid format. See [Working with Excel](excel.md#users-sheet) for details.

## Managing Collections

A Collection groups saved scenarios on one Version, to view them together in the Collections
app.

### Loading Collections

```python
from pyetm import Collection

# Load a single collection
collection = Collection.load(123)
print(f"Collection: {collection.title}")
print(f"Scenarios: {collection.saved_scenario_ids}")
print(f"View it at: {collection.collections_app_url}")

# Load all your collections
collections = Collection.load_all()
for collection in collections:
    print(f"{collection.id}: {collection.title}")
```

### Creating Collections

```python
from pyetm import Collection

collection = Collection.create(
    title="My Collection",
    saved_scenario_ids=[1, 2, 3],
)
```

!!! info "Collection Validation"
    - A collection needs at least one saved scenario.
    - Its `version` defaults to `ETM_VERSION`. Every scenario in it must be on that Version.

### Updating Collections

```python
collection.update(
    title="Updated Title",
    saved_scenario_ids=[1, 2, 3, 4],
)
```

### Deleting Collections

```python
# Permanent. The saved scenarios in it are not deleted.
collection.delete()
```
