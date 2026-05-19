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

# Logging
LOG_LEVEL=INFO

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

### LOG_LEVEL

Controls logging verbosity.

**Options:** `DEBUG`, `INFO` (default), `WARNING`, `ERROR`, `CRITICAL`

**Example:**

```env
LOG_LEVEL=INFO
```

### SSL Configuration

#### SSL_VERIFY

Enable or disable SSL certificate verification.

- **Default**: `true`
- **Set to `false` only for**: Testing with self-signed certificates in development

**Example:**

```env
SSL_VERIFY=false
```

!!! danger "Production Warning"
    Never disable SSL verification in production environments!

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
    log_level="DEBUG",
    ssl_verify=True,
)

# Initialize client with custom config
client = Client(config=config)
```

## Environment-Specific Setup

### Development Environment

```env
ETM_API_TOKEN=etm_dev.token.here
ENVIRONMENT=beta
LOG_LEVEL=DEBUG
SSL_VERIFY=true
```

### Production Environment

```env
ETM_API_TOKEN=etm_prod.token.here
ENVIRONMENT=pro
LOG_LEVEL=WARNING
SSL_VERIFY=true
TRUST_ENV=true
```

### Local Testing

```env
ETM_API_TOKEN=etm_local.token.here
ENVIRONMENT=local
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

!!! danger "Not for Production"
    Only use this in isolated development environments!

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
6. **Use `INFO` or `WARNING` log level in production**

## Next Steps

- [Quick Start Guide](quickstart.md) - Create your first scenario
- [Working with Scenarios](../user-guide/scenarios.md) - Learn about scenario management
- [API Reference](../api/) - Complete API documentation
