import os
import yaml
import pytest
from pathlib import Path
import pyetm.config.settings as settings_module
from pydantic import HttpUrl, ValidationError

AppConfig = settings_module.AppConfig
get_settings = settings_module.get_settings


# Fixture: clear any ENV vars
@pytest.fixture(autouse=True)
def clear_env(monkeypatch):
    for var in ("ETM_API_TOKEN", "BASE_URL", "LOG_LEVEL"):
        monkeypatch.delenv(var, raising=False)


# Helper to write a YAML file
def write_yaml(path: Path, data: dict):
    path.write_text(yaml.safe_dump(data))


# File has all values → use them
def test_from_yaml_loads_file_values(tmp_path):
    cfg_file = tmp_path / "config.yml"
    payload = {
        "etm_api_token": "etm_valid.looking.token",
        "base_url": "https://custom.local/api",
        "log_level": "DEBUG",
    }
    write_yaml(cfg_file, payload)

    config = AppConfig.from_yaml(cfg_file)

    assert config.etm_api_token == "etm_valid.looking.token"
    assert config.base_url == HttpUrl("https://custom.local/api")
    assert config.log_level == "DEBUG"


# File only has token; ENV overrides log_level; base_url uses default
def test_from_yaml_env_overrides_and_defaults(tmp_path, monkeypatch):
    cfg_file = tmp_path / "config.yml"
    write_yaml(cfg_file, {"etm_api_token": "etm_valid.looking.token"})

    # only override LOG_LEVEL
    monkeypatch.setenv("LOG_LEVEL", "WARNING")

    config = AppConfig.from_yaml(cfg_file)

    assert config.etm_api_token == "etm_valid.looking.token"
    assert config.log_level == "WARNING"
    # default from the class
    assert config.base_url == HttpUrl("https://engine.energytransitionmodel.com/api/v3")


# No file; ENV provides token; others default
def test_from_yaml_no_file_uses_env_and_defaults(tmp_path, monkeypatch):
    cfg_file = tmp_path / "does_not_exist.yml"
    monkeypatch.setenv("ETM_API_TOKEN", "etm_valid.looking.token")

    config = AppConfig.from_yaml(cfg_file)

    assert config.etm_api_token == "etm_valid.looking.token"
    assert config.base_url == HttpUrl("https://engine.energytransitionmodel.com/api/v3")
    assert config.log_level == "INFO"


# Invalid YAML is swallowed; ENV+defaults apply
def test_from_yaml_invalid_yaml_is_swallowed(tmp_path, monkeypatch):
    cfg_file = tmp_path / "config.yml"
    cfg_file.write_text(":\t not valid yaml :::")

    monkeypatch.setenv("ETM_API_TOKEN", "etm_valid.looking.token")

    config = AppConfig.from_yaml(cfg_file)

    assert config.etm_api_token == "etm_valid.looking.token"
    assert config.base_url == HttpUrl("https://engine.energytransitionmodel.com/api/v3")
    assert config.log_level == "INFO"


# Empty file + no ENV → get_settings() raises RuntimeError with helpful message
def test_get_settings_missing_token_raises_runtime_error(tmp_path, monkeypatch):
    cfg_file = tmp_path / "config.yml"
    write_yaml(cfg_file, {})

    monkeypatch.setattr(settings_module, "CONFIG_FILE", cfg_file)

    with pytest.raises(RuntimeError) as excinfo:
        get_settings()

    msg = str(excinfo.value)
    assert (
        "Configuration error: one or more required settings are missing or invalid"
        in msg
    )
    assert "• etm_api_token: Field required" in msg
    assert str(cfg_file) in msg


# VALID TOKENS
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
def test_valid_etm_api_token_regex(tmp_path, token):
    # Write a minimal config.yml with only the token
    cfg = tmp_path / "config.yml"
    write_yaml(cfg, {"etm_api_token": token})
    # Should not raise
    conf = AppConfig.from_yaml(cfg)
    assert conf.etm_api_token == token


# INVALID TOKENS
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
def test_invalid_etm_api_token_raises(tmp_path, token):
    cfg = tmp_path / "config.yml"
    write_yaml(cfg, {"etm_api_token": token})
    with pytest.raises(ValidationError) as excinfo:
        AppConfig.from_yaml(cfg)
    errs = excinfo.value.errors()
    # Should have exactly one error, on the token field
    assert any(err["loc"] == ("etm_api_token",) for err in errs)
    assert any("Invalid ETM API token" in err["msg"] for err in errs)
