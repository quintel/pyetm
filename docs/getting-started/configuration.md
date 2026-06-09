# Configuration

pyetm uses environment variables and configuration files to manage settings for API access, SSL certificates, and other options.

## Configuration Methods

pyetm supports multiple ways to configure your environment:

1. **Environment file** (`.env`) - Recommended for local development
2. **Environment variables** - Useful for CI/CD and production deployments
3. **Programmatic configuration** - For advanced use cases

## Environment File (.env)

The easiest way to configure pyetm is with a `.env` file in your project root. The `pyetm init` command will create this for you:

```bash
pyetm init
```

### Example .env File

```env
# ETM API Authentication
ETM_API_TOKEN=etm_your.token.here

# Environment Selection
ENVIRONMENT=pro

# Error Handling (optional)
PYETM_ERROR_MODE=default

# SSL Configuration (optional)
SSL_VERIFY=true
# SSL_CERT_PATH=/path/to/custom/ca-bundle.crt

# CSV Settings (optional)
CSV_SEPARATOR=,
DECIMAL_SEPARATOR=.
```

## Configuration Options

### ETM_API_TOKEN

Your personal ETM API token for authentication.

- **Required for**: Saving scenarios, accessing private scenarios
- **Optional for**: Creating and working with public scenarios
- **Format**: Must start with `etm_` prefix
- **Get your token**: [ETM API Authentication Docs](https://docs.energytransitionmodel.com/api/authentication)

!!! warning "Security"
    Never commit your `.env` file to version control! Add it to `.gitignore`.

### ENVIRONMENT

Specifies which ETM environment to connect to.

**Options:**

- `pro` (default) - Production environment ([energytransitionmodel.com](https://energytransitionmodel.com))
- `beta` - Beta testing environment
- `local` - Local development server
- `YYYY-MM` - Stable release tag (e.g., `2024-12`)

**Example:**

```env
ENVIRONMENT=pro
```

### BASE_URL

Custom API base URL (advanced users only).

- **Default**: Inferred from `ENVIRONMENT` setting
- **Use case**: Custom or self-hosted ETM instances

**Example:**

```env
BASE_URL=https://custom-etm.example.com/api/v3
```

### PYETM_ERROR_MODE

Controls how pyetm handles errors and warnings during operations.

**Options:**

- `safe` - All warnings raise exceptions (maximum safety)
- `default` (default) - Smart behavior: errors raise in single operations, collected in bulk
- `dangerous` - No warnings raise, all logged only

**When to use each mode:**

**`default` mode (recommended for most users):**

Single operations raise on errors immediately:
```python
scenario.update(inputs={"invalid_key": 123})  # Raises RuntimeError
```

Bulk operations collect warnings and continue:
```python
scenarios = Scenarios.create_many([...])  # Continues processing
scenarios.show_warnings()  # Display collected warnings
```

**`safe` mode (recommended for CI/production):**

All warnings raise exceptions, even in bulk operations. Use this when you want maximum safety and can't tolerate any partial failures.

```env
PYETM_ERROR_MODE=safe
```

**`dangerous` mode (for exploratory data analysis):**

Nothing raises exceptions - all warnings are logged. Useful when you want to process as much data as possible and handle errors afterwards.

```env
PYETM_ERROR_MODE=dangerous
```

!!! note "Relationship with LOG_LEVEL"
    `PYETM_ERROR_MODE` and `LOG_LEVEL` are independent:

    - `PYETM_ERROR_MODE` controls **operational flow** (raise exceptions vs. collect warnings)
    - `LOG_LEVEL` controls **system logging verbosity** (what gets logged by Python's logger)

    User-facing warnings from `show_warnings()` are always displayed regardless of `LOG_LEVEL`.

**Example:**

```env
PYETM_ERROR_MODE=default
```

### LOG_LEVEL

Controls logging verbosity for system/debug messages. This is an advanced option for debugging.

**Options:** `DEBUG`, `INFO` (default), `WARNING`, `ERROR`, `CRITICAL`

**Example:**

```env
LOG_LEVEL=INFO
```

!!! tip "Advanced Users Only"
    Most users don't need to change `LOG_LEVEL`. It controls internal logging, not user-facing warnings. Use `PYETM_ERROR_MODE` to control error handling behavior instead.

### SSL Configuration

#### SSL_VERIFY

Enable or disable SSL certificate verification.

- **Default**: `true`
- **Set to `false` only for**: Testing with self-signed certificates in development

**Example:**

```env
SSL_VERIFY=false
```

#### SSL_CERT_PATH

Path to a custom CA certificate bundle for SSL verification.

- **Use case**: Corporate networks with custom CA certificates
- **Format**: Path to a `.crt` or `.pem` file

**Example:**

```env
SSL_CERT_PATH=/etc/ssl/certs/custom-ca-bundle.crt
```

### TRUST_ENV

Trust system environment proxy settings.

- **Default**: `false`
- **Reads**: `HTTP_PROXY`, `HTTPS_PROXY`, `NO_PROXY` environment variables

**Example:**

```env
TRUST_ENV=true
```

### CSV Settings

#### CSV_SEPARATOR

Character used to separate CSV columns.

- **Default**: `,` (comma)
- **Common alternatives**: `;` (semicolon) for European formats

**Example:**

```env
CSV_SEPARATOR=;
```

#### DECIMAL_SEPARATOR

Character used for decimal points in numbers.

- **Default**: `.` (period)
- **Common alternatives**: `,` (comma) for European formats

**Example:**

```env
DECIMAL_SEPARATOR=,
```

## Programmatic Configuration

For advanced use cases, you can configure pyetm programmatically:

```python
from pyetm import Client
from pyetm.config import AppConfig

# Create custom configuration
config = AppConfig(
    etm_api_token="etm_your.token.here",
    environment="pro",
    error_mode="default",  # or "safe" or "dangerous"
    log_level="DEBUG",
    ssl_verify=True,
)

# Initialize client with custom config
client = Client(config=config)
```

## Runtime Configuration Changes

For testing or interactive workflows, you may need to reload configuration from environment variables after they've been modified at runtime.

### reload_configuration()

Use the `reload_configuration()` function to reload all settings from environment variables:

```python
import os
from pyetm.config import reload_configuration

# Modify environment variable
os.environ["PYETM_ERROR_MODE"] = "dangerous"

# Reload configuration to pick up changes
reload_configuration()

# Settings now reflect the new environment
```

This function clears both the settings cache and the error policy cache, ensuring that the next operation will use the updated configuration.

**When to use:**

- **Testing**: Switch between error modes or environments during test runs
- **Interactive notebooks**: Dynamically adjust configuration based on analysis needs
- **Runtime adjustments**: Change settings without restarting your Python session

**Important notes:**

!!! warning "Production Use"
    In production code, configuration should typically be set once at startup via environment variables or `.env` files. Runtime configuration changes modify global state and should be used cautiously.

!!! tip "Alternative: Programmatic Configuration"
    For most use cases, prefer programmatic configuration (see above) over runtime reloading. This provides better control and avoids global state modifications.

**Example: Testing with different error modes**

```python
import os
from pyetm.config import reload_configuration
from pyetm import Scenarios

# Test with default mode
scenarios = Scenarios.create_many([...])  # Collects warnings

# Switch to safe mode for next test
os.environ["PYETM_ERROR_MODE"] = "safe"
reload_configuration()

# Now warnings will raise exceptions
scenarios = Scenarios.create_many([...])  # Raises on first warning
```

## Environment-Specific Setup

### Development Environment

```env
ETM_API_TOKEN=etm_dev.token.here
ENVIRONMENT=beta
PYETM_ERROR_MODE=default
LOG_LEVEL=DEBUG
SSL_VERIFY=true
```

### Production Environment

```env
ETM_API_TOKEN=etm_prod.token.here
ENVIRONMENT=pro
PYETM_ERROR_MODE=safe
LOG_LEVEL=WARNING
SSL_VERIFY=true
TRUST_ENV=true
```

### Data Analysis / Exploratory

```env
ETM_API_TOKEN=etm_your.token.here
ENVIRONMENT=pro
PYETM_ERROR_MODE=dangerous
LOG_LEVEL=INFO
```

### Local Testing

```env
ETM_API_TOKEN=etm_local.token.here
ENVIRONMENT=local
PYETM_ERROR_MODE=default
LOG_LEVEL=DEBUG
SSL_VERIFY=false
BASE_URL=http://localhost:3000/api/v3
```

## SSL Certificate Issues

If you encounter SSL certificate errors, try these solutions:

### 1. Update certifi

```bash
pip install --upgrade certifi
```

### 2. Use Custom CA Bundle

For corporate networks with custom CAs:

```bash
# Download your organization's CA certificate
# Then configure pyetm to use it:
```

```env
SSL_CERT_PATH=/path/to/corporate-ca-bundle.crt
```

### 3. Disable SSL Verification (Development Only)

```env
SSL_VERIFY=false
```

### 4. System Certificates

On macOS with corporate proxies:

```bash
# Export system certificates
security find-certificate -a -p /System/Library/Keychains/SystemRootCertificates.keychain > /tmp/ca-bundle.crt

# Configure pyetm
```

```env
SSL_CERT_PATH=/tmp/ca-bundle.crt
```

## Validation

pyetm validates your configuration on startup. If you have issues, you can test your config:

```python
from pyetm.config import AppConfig

try:
    config = AppConfig()
    print("Configuration valid!")
    print(f"Environment: {config.environment}")
    print(f"Base URL: {config.base_url}")
except Exception as e:
    print(f"Configuration error: {e}")
```

## Configuration Priority

When multiple configuration sources exist, pyetm uses this priority order:

1. **Programmatic configuration** (highest priority)
2. **Environment variables**
3. **`.env` file**
4. **Default values** (lowest priority)

## Best Practices

1. **Use `.env` files for local development**
2. **Use environment variables for CI/CD and production**
3. **Never commit tokens or `.env` files to version control**
4. **Use different tokens for different environments**
5. **Keep SSL verification enabled in production**
6. **Use `safe` error mode in CI/production environments**
7. **Use `default` error mode for most development work**
8. **Use `dangerous` mode only for exploratory data analysis**

## Next Steps

- [Quick Start Guide](quickstart.md) - Create your first scenario
- [Working with Scenarios](../user-guide/scenarios.md) - Learn about scenario management
- [API Reference](../api/index.md) - Complete API documentation
