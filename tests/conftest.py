"""
Runs during test collection. You can also supply fixtures here that should be loaded
before each test
"""

import os, sys, pytest
from pathlib import Path

# Ensure src/ is on sys.path before any imports of your app code
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC = os.path.join(ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

os.environ["ETM_ENVIRONMENT"] = "pro"
os.environ["ETM_API_TOKEN"] = "etm_real.looking.token"


# Clear LRU caches after each test to prevent test pollution
@pytest.fixture(autouse=True)
def clear_client_caches():
    """Clear client and settings caches after each test to prevent pollution."""
    yield  # Run the test
    # Clear caches after test completes
    from pyetm.clients.base_client import get_client
    from pyetm.config.settings import reload_configuration

    if get_client.cache_info().currsize:
        get_client().close()
    get_client.cache_clear()
    reload_configuration()


# Lazy‐import BaseClient
@pytest.fixture
def client():
    from pyetm.clients.base_client import BaseClient

    # Create a new client instance for each test (not using get_client())
    client = BaseClient()
    yield client
    client.close()


@pytest.fixture
def scenario():
    from pyetm.models import Scenario

    return Scenario(id=999)


# --- Config Fixtures for Testing Optional Token and Custom Environments --- #


@pytest.fixture
def clean_settings_env(monkeypatch, tmp_path):
    """Create a completely clean environment for settings tests"""
    from pyetm.config.settings import reload_configuration

    # Clear caches to ensure each test gets a fresh config
    reload_configuration()

    etm_vars = [
        "ETM_API_TOKEN",
        "BASE_URL",
        "LOG_LEVEL",
        "ETM_ENVIRONMENT",
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


@pytest.fixture
def unauthenticated_config(monkeypatch):
    """Fixture for testing unauthenticated (no token) configuration"""
    from pyetm.config.settings import AppConfig, get_settings

    # Clear cache to ensure fresh config with test settings
    get_settings.cache_clear()

    monkeypatch.delenv("ETM_API_TOKEN", raising=False)
    monkeypatch.setenv("ETM_ENVIRONMENT", "pro")

    # Explicitly set etm_api_token to None to override .env file
    return AppConfig(etm_api_token=None)


# --- Service Result Fixtures --- #


@pytest.fixture
def ok_service_result():
    """Factory fixture for creating successful ServiceResult objects"""
    from pyetm.services.service_result import ServiceResult

    def _make_result(data, errors=None):
        return ServiceResult.ok(data=data, errors=errors or [])

    return _make_result


@pytest.fixture
def fail_service_result():
    """Factory fixture for creating failed ServiceResult objects"""
    from pyetm.services.service_result import ServiceResult

    def _make_result(errors):
        return ServiceResult.fail(errors)

    return _make_result
