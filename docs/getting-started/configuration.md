# Configuration

pyetm reads its settings from a `.env` file in your working directory, or from environment variables. `pyetm init` creates the file for you:

```bash
pyetm init
```

Most people only need a token:

```env
ETM_API_TOKEN=etm_your.token.here
```

## ETM_API_TOKEN

Your personal token. Create one on your [tokens page](https://my.energytransitionmodel.com/identity/tokens).

- **Without a token** you can read public scenarios.
- **With a token** you can also create, change, list and delete your scenarios, and read private ones shared with you. What a token may do is chosen when you create it.

!!! warning
    Never commit your `.env` file. Add it to `.gitignore`.

## ETM_ENVIRONMENT

Which ETM to talk to: `pro` (the default) or `beta`.

```env
ETM_ENVIRONMENT=beta
```

pyetm 2.1 is tested on the `latest` Version of the ETM, and can work on other Versions through the same API. For `2025-01`, install pyetm 2.0.x. See [Compatibility](compatibility.md).

## PYETM_ERROR_MODE

How pyetm reacts to problems:

| Mode | Behaviour |
|---|---|
| `default` | A single operation raises. A bulk operation, such as `Scenarios.create_many()` or an Excel import, collects warnings and carries on. |
| `safe` | Every warning raises. Use it where a partial failure is unacceptable. |
| `dangerous` | Nothing raises. Every warning is logged. |

## Other settings

| Setting | Default | Use |
|---|---|---|
| `ETM_VERSION` | `latest` | The Version new scenarios are created on |
| `LOG_LEVEL` | `INFO` | Python logging verbosity, for debugging |
| `CSV_SEPARATOR`, `DECIMAL_SEPARATOR` | `,` and `.` | Separators for CSV files |
| `SSL_CERT_PATH` | | A CA bundle, for networks with their own certificates |
| `TRUST_ENV` | `false` | Use the `HTTP_PROXY` and `HTTPS_PROXY` variables |

## Several tokens or ETMs in one script

Give a scenario its own client. Every call on that scenario then uses it:

```python
from pyetm import BaseClient, Scenario

client = BaseClient(token="etm_other.token.here")
scenario = Scenario.load(123, client=client)
```

After changing an environment variable in a running session, call `pyetm.config.reload_configuration()` to pick it up.
