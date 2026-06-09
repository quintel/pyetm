import pytest
from pathlib import Path
import pyetm.config.settings as settings_module
from pydantic import HttpUrl, ValidationError

AppConfig = settings_module.AppConfig
get_settings = settings_module.get_settings


# Settings-specific fixture for clean environment
@pytest.fixture
def clean_settings_env(monkeypatch, tmp_path):
    """Create a completely clean environment for settings tests"""
    # Clear the get_settings cache to ensure each test gets a fresh config
    get_settings.cache_clear()

    # Clear all ETM environment variables
    etm_vars = [
        "ETM_API_TOKEN",
        "BASE_URL",
        "LOG_LEVEL",
        "ENVIRONMENT",
        "CSV_SEPARATOR",
        "DECIMAL_SEPARATOR",
        "SSL_VERIFY",
        "TRUST_ENV",
        "SSL_CERT_PATH",
        "PYETM_ERROR_MODE",
    ]
    for var in etm_vars:
        monkeypatch.delenv(var, raising=False)

    # Create isolated .env file in temp directory
    test_env_file = tmp_path / ".env"

    # Change to the temp directory so AppConfig finds our test .env file
    monkeypatch.chdir(tmp_path)

    return test_env_file


# Helper to write a .env file
def write_env_file(path: Path, data: dict):
    lines = []
    for key, value in data.items():
        # Quote values with spaces
        if isinstance(value, str) and (" " in value or "#" in value):
            value = f'"{value}"'
        lines.append(f"{key}={value}")
    path.write_text("\n".join(lines))


# Test basic .env file loading
def test_config_loads_env_file_values(clean_settings_env):
    env_file = clean_settings_env
    env_data = {
        "ETM_API_TOKEN": "etm_valid.looking.token",
        "BASE_URL": "https://custom.local/api",
        "LOG_LEVEL": "DEBUG",
        "ENVIRONMENT": "beta",
        "CSV_SEPARATOR": ";",
        "DECIMAL_SEPARATOR": ",",
    }
    write_env_file(env_file, env_data)

    config = AppConfig()

    assert config.etm_api_token == "etm_valid.looking.token"
    assert config.base_url == HttpUrl("https://custom.local/api")
    assert config.log_level == "DEBUG"
    assert config.environment == "beta"
    assert config.csv_separator == ";"
    assert config.decimal_separator == ","


# Test environment variables override .env file
def test_env_vars_override_env_file(clean_settings_env, monkeypatch):
    env_file = clean_settings_env
    write_env_file(env_file, {"ETM_API_TOKEN": "etm_from.env.file", "LOG_LEVEL": "DEBUG"})

    # ENV var should override file
    monkeypatch.setenv("LOG_LEVEL", "WARNING")

    config = AppConfig()

    assert config.etm_api_token == "etm_from.env.file"  # from file
    assert config.log_level == "WARNING"  # from env var (overrides file)


# Test base_url inference from environment
def test_base_url_inference_from_environment(clean_settings_env):
    env_file = clean_settings_env
    write_env_file(env_file, {"ETM_API_TOKEN": "etm_valid.looking.token", "ENVIRONMENT": "beta"})

    config = AppConfig()

    assert config.environment == "beta"
    assert config.base_url == HttpUrl("https://beta.engine.energytransitionmodel.com/api/v3")


# Test no .env file, only environment variables
def test_no_env_file_uses_env_vars_and_defaults(clean_settings_env, monkeypatch):
    # Don't create the env_file, just set environment variable
    monkeypatch.setenv("ETM_API_TOKEN", "etm_valid.looking.token")

    config = AppConfig()

    assert config.etm_api_token == "etm_valid.looking.token"
    assert config.base_url == HttpUrl("https://engine.energytransitionmodel.com/api/v3")
    assert config.log_level == "INFO"
    assert config.environment == "pro"


# Test optional token - no token is now valid
def test_optional_token_no_error(clean_settings_env, caplog):
    """Test that missing token is allowed and generates a warning"""
    import logging

    env_file = clean_settings_env
    write_env_file(env_file, {})

    with caplog.at_level(logging.WARNING):
        config = AppConfig()

    # Token should be None
    assert config.etm_api_token is None

    # Should have logged a warning
    assert any(
        "No ETM_API_TOKEN provided" in record.message and "public scenarios" in record.message
        for record in caplog.records
    )


# Test get_settings with missing token
def test_get_settings_missing_token_succeeds(clean_settings_env):
    """Test that get_settings() succeeds when token is missing"""
    env_file = clean_settings_env
    write_env_file(env_file, {})

    # Should not raise
    config = get_settings()
    assert config.etm_api_token is None


# Test defaults when no configuration provided
def test_default_values(clean_settings_env):
    env_file = clean_settings_env
    write_env_file(env_file, {"ETM_API_TOKEN": "etm_valid.looking.token"})

    config = AppConfig()

    assert config.etm_api_token == "etm_valid.looking.token"
    assert config.environment == "pro"
    assert config.log_level == "INFO"
    assert config.csv_separator == ","
    assert config.decimal_separator == "."
    assert config.base_url == HttpUrl("https://engine.energytransitionmodel.com/api/v3")


# Test environment inference for different values
@pytest.mark.parametrize(
    "env,expected_url",
    [
        ("pro", "https://engine.energytransitionmodel.com/api/v3"),
        ("beta", "https://beta.engine.energytransitionmodel.com/api/v3"),
        ("local", "http://localhost:3000/api/v3"),
        ("2025-01", "https://2025-01.engine.energytransitionmodel.com/api/v3"),
        ("", "https://engine.energytransitionmodel.com/api/v3"),  # default
        # Custom environments now resolve to subdomain
        ("tyndp2024", "https://tyndp2024.engine.energytransitionmodel.com/api/v3"),
        ("tyndp2026", "https://tyndp2026.engine.energytransitionmodel.com/api/v3"),
        (
            "custom-env",
            "https://custom-env.engine.energytransitionmodel.com/api/v3",
        ),
    ],
)
def test_environment_inference(clean_settings_env, env, expected_url):
    env_file = clean_settings_env
    env_data = {}
    if env:  # Don't add environment key if it's empty string
        env_data["ENVIRONMENT"] = env

    write_env_file(env_file, env_data)

    config = AppConfig()
    assert config.base_url == HttpUrl(expected_url)


# Test explicit base_url overrides environment inference
def test_explicit_base_url_overrides_environment(clean_settings_env):
    env_file = clean_settings_env
    write_env_file(
        env_file,
        {
            "ETM_API_TOKEN": "etm_valid.looking.token",
            "ENVIRONMENT": "beta",
            "BASE_URL": "https://custom.override.com/api/v3",
        },
    )

    config = AppConfig()

    assert config.environment == "beta"
    assert config.base_url == HttpUrl("https://custom.override.com/api/v3")


# VALID TOKENS
@pytest.mark.parametrize(
    "token",
    [
        # JWT tokens
        "etm_eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6"
        "IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c",
        # with _beta
        "etm_beta_eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6"
        "IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c",
        # Non-JWT tokens (custom tokens for non-standard environments)
        "etm_custom_token_123",
        "etm_simpletoken",
        "etm_beta_custom_token",
    ],
)
def test_valid_etm_api_token_regex(clean_settings_env, token):
    env_file = clean_settings_env
    write_env_file(env_file, {"ETM_API_TOKEN": token})

    # Should not raise
    config = AppConfig()
    assert config.etm_api_token == token


# INVALID TOKENS
@pytest.mark.parametrize(
    "token",
    [
        # missing prefix entirely
        "eyJhbGciOiJIUzI1NiJ9.header.payload.signature",
        # double underscore (body starts with non-alphanumeric)
        "etm__eyJhbGci.abc.def",
        # invalid JWT with 2 parts (has dots but not 3 segments)
        "etm_eyJhbGci.eyJzdWIiOiIxMjM0NTY",
        # invalid JWT with spaces (has 3 parts but contains spaces)
        "etm_beta_eyJhbGci.eyJ zdWIi.abc",
        # empty body after prefix
        "etm_",
        "etm_beta_",
    ],
)
def test_invalid_etm_api_token_raises(clean_settings_env, token):
    env_file = clean_settings_env
    write_env_file(env_file, {"ETM_API_TOKEN": token})

    with pytest.raises(ValidationError) as excinfo:
        AppConfig()
    errs = excinfo.value.errors()
    # Should have exactly one error, on the token field
    assert any(err["loc"] == ("etm_api_token",) for err in errs)
    assert any("Invalid ETM API token" in err["msg"] for err in errs)


# Test temp folder functionality
def test_path_to_tmp_creates_directory(clean_settings_env):
    env_file = clean_settings_env
    write_env_file(env_file, {"ETM_API_TOKEN": "etm_valid.looking.token"})

    config = AppConfig()
    config.temp_folder = env_file.parent / "custom_tmp"

    result_path = config.path_to_tmp("test_subfolder")

    assert result_path.exists()
    assert result_path.is_dir()
    assert result_path.name == "test_subfolder"
    assert result_path.parent == config.temp_folder


# Test quoted values in .env file
def test_quoted_values_in_env_file(clean_settings_env):
    env_file = clean_settings_env
    content = '''ETM_API_TOKEN=etm_valid.looking.token
LOG_LEVEL="DEBUG WITH SPACES"
CSV_SEPARATOR=";"'''
    env_file.write_text(content)

    config = AppConfig()

    assert config.etm_api_token == "etm_valid.looking.token"
    assert config.log_level == "DEBUG WITH SPACES"
    assert config.csv_separator == ";"


# Test SSL default values
def test_ssl_default_values(clean_settings_env):
    env_file = clean_settings_env
    write_env_file(env_file, {"ETM_API_TOKEN": "etm_valid.looking.token"})

    config = AppConfig()

    assert config.ssl_verify is True  # Should verify by default
    assert config.trust_env is False  # Should not trust env by default
    assert config.ssl_cert_path is None  # No custom cert by default


# Test SSL verification can be disabled
def test_ssl_verify_disabled(clean_settings_env):
    env_file = clean_settings_env
    write_env_file(
        env_file,
        {
            "ETM_API_TOKEN": "etm_valid.looking.token",
            "SSL_VERIFY": "false",
        },
    )

    config = AppConfig()

    assert config.ssl_verify is False


# Test trust_env can be enabled
def test_trust_env_enabled(clean_settings_env):
    env_file = clean_settings_env
    write_env_file(
        env_file,
        {
            "ETM_API_TOKEN": "etm_valid.looking.token",
            "TRUST_ENV": "true",
        },
    )

    config = AppConfig()

    assert config.trust_env is True


# Test custom SSL certificate path
def test_ssl_cert_path_valid(clean_settings_env, tmp_path):
    env_file = clean_settings_env

    # Create a dummy cert file
    cert_file = tmp_path / "ca-bundle.crt"
    cert_file.write_text("-----BEGIN CERTIFICATE-----\nMOCK\n-----END CERTIFICATE-----")

    write_env_file(
        env_file,
        {
            "ETM_API_TOKEN": "etm_valid.looking.token",
            "SSL_CERT_PATH": str(cert_file),
        },
    )

    config = AppConfig()

    assert config.ssl_cert_path == cert_file
    assert config.ssl_cert_path.exists()


# Test invalid SSL certificate path raises error
def test_ssl_cert_path_invalid_raises_error(clean_settings_env, tmp_path):
    env_file = clean_settings_env

    # Use a path that doesn't exist
    fake_cert_path = tmp_path / "nonexistent.crt"

    write_env_file(
        env_file,
        {
            "ETM_API_TOKEN": "etm_valid.looking.token",
            "SSL_CERT_PATH": str(fake_cert_path),
        },
    )

    with pytest.raises(ValidationError) as excinfo:
        AppConfig()

    errs = excinfo.value.errors()
    assert any("SSL certificate file not found" in str(err) for err in errs)


# Test all SSL options together
def test_all_ssl_options_together(clean_settings_env, tmp_path):
    env_file = clean_settings_env

    cert_file = tmp_path / "custom-ca.crt"
    cert_file.write_text("-----BEGIN CERTIFICATE-----\nTEST\n-----END CERTIFICATE-----")

    write_env_file(
        env_file,
        {
            "ETM_API_TOKEN": "etm_valid.looking.token",
            "SSL_VERIFY": "true",
            "TRUST_ENV": "true",
            "SSL_CERT_PATH": str(cert_file),
        },
    )

    config = AppConfig()

    assert config.ssl_verify is True
    assert config.trust_env is True
    assert config.ssl_cert_path == cert_file
