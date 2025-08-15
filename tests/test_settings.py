import os
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
    # Clear all ETM environment variables
    etm_vars = [
        "ETM_API_TOKEN",
        "BASE_URL",
        "LOG_LEVEL",
        "ENVIRONMENT",
        "CSV_SEPARATOR",
        "DECIMAL_SEPARATOR",
        "PROXY_SERVERS_HTTP",
        "PROXY_SERVERS_HTTPS",
    ]
    for var in etm_vars:
        monkeypatch.delenv(var, raising=False)

    # Create isolated config file path
    test_config_file = tmp_path / "isolated_config.env"
    monkeypatch.setattr(settings_module, "ENV_FILE", test_config_file)

    return test_config_file


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
    write_env_file(
        env_file, {"ETM_API_TOKEN": "etm_from.env.file", "LOG_LEVEL": "DEBUG"}
    )

    # ENV var should override file
    monkeypatch.setenv("LOG_LEVEL", "WARNING")

    config = AppConfig()

    assert config.etm_api_token == "etm_from.env.file"  # from file
    assert config.log_level == "WARNING"  # from env var (overrides file)


# Test base_url inference from environment
def test_base_url_inference_from_environment(clean_settings_env):
    env_file = clean_settings_env
    write_env_file(
        env_file, {"ETM_API_TOKEN": "etm_valid.looking.token", "ENVIRONMENT": "beta"}
    )

    config = AppConfig()

    assert config.environment == "beta"
    assert config.base_url == HttpUrl(
        "https://beta.engine.energytransitionmodel.com/api/v3"
    )


# Test proxy servers configuration
def test_proxy_servers_configuration(clean_settings_env):
    env_file = clean_settings_env
    write_env_file(
        env_file,
        {
            "ETM_API_TOKEN": "etm_valid.looking.token",
            "PROXY_SERVERS_HTTP": "http://proxy.example.com:8080",
            "PROXY_SERVERS_HTTPS": "https://secure.proxy.com:8080",
        },
    )

    config = AppConfig()

    assert config.proxy_servers_http == "http://proxy.example.com:8080"
    assert config.proxy_servers_https == "https://secure.proxy.com:8080"

    # Test backward compatibility property
    proxy_dict = config.proxy_servers
    assert proxy_dict["http"] == "http://proxy.example.com:8080"
    assert proxy_dict["https"] == "https://secure.proxy.com:8080"


# Test no .env file, only environment variables
def test_no_env_file_uses_env_vars_and_defaults(clean_settings_env, monkeypatch):
    # Don't create the env_file, just set environment variable
    monkeypatch.setenv("ETM_API_TOKEN", "etm_valid.looking.token")

    config = AppConfig()

    assert config.etm_api_token == "etm_valid.looking.token"
    assert config.base_url == HttpUrl("https://engine.energytransitionmodel.com/api/v3")
    assert config.log_level == "INFO"
    assert config.environment == "pro"


# Test missing required token raises helpful error
def test_get_settings_missing_token_raises_runtime_error(clean_settings_env):
    env_file = clean_settings_env
    write_env_file(env_file, {})

    with pytest.raises(RuntimeError) as excinfo:
        get_settings()

    msg = str(excinfo.value)
    assert (
        "Configuration error: one or more required settings are missing or invalid"
        in msg
    )
    assert "• etm_api_token: Field required" in msg
    assert str(env_file) in msg


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
    assert config.proxy_servers_http is None
    assert config.proxy_servers_https is None
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
        ("unknown", "https://engine.energytransitionmodel.com/api/v3"),  # fallback
    ],
)
def test_environment_inference(clean_settings_env, env, expected_url):
    env_file = clean_settings_env
    env_data = {"ETM_API_TOKEN": "etm_valid.looking.token"}
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


# VALID TOKENS (same as before)
@pytest.mark.parametrize(
    "token",
    [
        # minimal JWT chars + no beta
        "etm_eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6"
        "IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c",
        # with _beta
        "etm_beta_eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6"
        "IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c",
    ],
)
def test_valid_etm_api_token_regex(clean_settings_env, token):
    env_file = clean_settings_env
    write_env_file(env_file, {"ETM_API_TOKEN": token})

    # Should not raise
    config = AppConfig()
    assert config.etm_api_token == token


# INVALID TOKENS (same as before)
@pytest.mark.parametrize(
    "token",
    [
        # missing prefix entirely
        "eyJhbGciOiJIUzI1NiJ9.header.payload.signature",
        # double underscore because you made beta optional incorrectly
        "etm__eyJhbGci.abc.def",
        # only two parts
        "etm_eyJhbGci.eyJzdWIiOiIxMjM0NTY",
        # invalid characters (space)
        "etm_beta_eyJhbGci.eyJ zdWIi.abc",
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
    # Manually write with quotes to test parsing
    content = '''ETM_API_TOKEN=etm_valid.looking.token
LOG_LEVEL="DEBUG WITH SPACES"
CSV_SEPARATOR=";"
PROXY_SERVERS_HTTP="http://user:pass@proxy.example.com:8080"'''
    env_file.write_text(content)

    config = AppConfig()

    assert config.etm_api_token == "etm_valid.looking.token"
    assert config.log_level == "DEBUG WITH SPACES"
    assert config.csv_separator == ";"
    assert config.proxy_servers_http == "http://user:pass@proxy.example.com:8080"
