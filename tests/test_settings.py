import os
import yaml
import pytest
from pathlib import Path
from pydantic import ValidationError
from pyetm.config.settings import AppConfig

# Fixture: clear any ENV vars
@pytest.fixture(autouse=True)
def clear_env(monkeypatch):
    for var in ("ETM_API_TOKEN", "BASE_URL", "LOG_LEVEL"):
        monkeypatch.delenv(var, raising=False)

# Helper to write a YAML file
def write_yaml(path: Path, data: dict):
    path.write_text(yaml.safe_dump(data))

# 1) File has all values → use them
def test_from_yaml_loads_file_values(tmp_path):
    cfg_file = tmp_path / "config.yml"
    payload = {
        "etm_api_token": "file-token",
        "base_url": "https://custom.local/api",
        "log_level": "DEBUG",
    }
    write_yaml(cfg_file, payload)

    config = AppConfig.from_yaml(cfg_file)

    assert config.etm_api_token == "file-token"
    assert config.base_url == "https://custom.local/api"
    assert config.log_level == "DEBUG"

# 2) File only has token; ENV overrides log_level; base_url uses default
def test_from_yaml_env_overrides_and_defaults(tmp_path, monkeypatch):
    cfg_file = tmp_path / "config.yml"
    write_yaml(cfg_file, {"etm_api_token": "file-token"})

    # only override LOG_LEVEL
    monkeypatch.setenv("LOG_LEVEL", "WARNING")

    config = AppConfig.from_yaml(cfg_file)

    assert config.etm_api_token == "file-token"
    assert config.log_level == "WARNING"
    # default from the class
    assert config.base_url == "https://engine.energytransitionmodel.com/api/v3"

# 3) No file; ENV provides token; others default
def test_from_yaml_no_file_uses_env_and_defaults(tmp_path, monkeypatch):
    cfg_file = tmp_path / "does_not_exist.yml"
    monkeypatch.setenv("ETM_API_TOKEN", "env-token")

    config = AppConfig.from_yaml(cfg_file)

    assert config.etm_api_token == "env-token"
    assert config.base_url == "https://engine.energytransitionmodel.com/api/v3"
    assert config.log_level == "INFO"

# 4) Invalid YAML is swallowed; ENV+defaults apply
def test_from_yaml_invalid_yaml_is_swallowed(tmp_path, monkeypatch):
    cfg_file = tmp_path / "config.yml"
    cfg_file.write_text(":\t not valid yaml :::")

    monkeypatch.setenv("ETM_API_TOKEN", "env-token")

    config = AppConfig.from_yaml(cfg_file)

    assert config.etm_api_token == "env-token"
    assert config.base_url == "https://engine.energytransitionmodel.com/api/v3"
    assert config.log_level == "INFO"

# 5) Empty file + no ENV → missing required token → ValidationError
def test_from_yaml_missing_token_raises_validation_error(tmp_path):
    cfg_file = tmp_path / "config.yml"
    write_yaml(cfg_file, {})  # no fields

    with pytest.raises(ValidationError) as excinfo:
        AppConfig.from_yaml(cfg_file)

    assert "etm_api_token" in str(excinfo.value)
