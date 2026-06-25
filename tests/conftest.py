"""
Runs during test collection. You can also supply fixtures here that should be loaded
before each test
"""

from pydantic import HttpUrl
import os, sys, pytest
from pathlib import Path

# Ensure src/ is on sys.path before any imports of your app code
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC = os.path.join(ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

os.environ["BASE_URL"] = "https://example.com/api"
os.environ["ETM_API_TOKEN"] = "etm_real.looking.token"


# Fixture to give back that same base URL for building expected mock URLs
@pytest.fixture
def api_url():
    return HttpUrl(os.getenv("BASE_URL"))


# Mount the requests-mock adapter onto BaseClient.session so that
# requests_mock.get(...) actually intercepts client.session.get(...)
@pytest.fixture(autouse=True)
def _mount_requests_mock(request, requests_mock):
    """
    requests_mock._adapter is the HTTPAdapter instance used
    by the pytest-requests-mock plugin.

    Only mounts if the test uses the 'client' fixture to avoid
    unnecessary BaseClient creation and threading overhead.
    """
    if "client" not in request.fixturenames:
        return

    client = request.getfixturevalue("client")
    adapter = getattr(requests_mock, "_adapter", None)
    if adapter and hasattr(client, "session") and hasattr(client.session, "session"):
        client.session.session.mount("http://", adapter)
        client.session.session.mount("https://", adapter)


# Clear LRU caches after each test to prevent test pollution
@pytest.fixture(autouse=True)
def clear_client_caches():
    """Clear client and settings caches after each test to prevent pollution."""
    yield  # Run the test
    # Clear caches after test completes
    from pyetm.clients.base_client import get_client
    from pyetm.config.settings import reload_configuration

    get_client.cache_clear()
    reload_configuration()


# Mock CurveMetadataService to prevent API calls during tests
@pytest.fixture(autouse=True)
def mock_curve_metadata_service(request, monkeypatch):
    """Mock CurveMetadataService to return test data without making API calls."""
    # Skip mocking for tests that specifically test the CurveMetadataService
    if "test_curve_metadata_service" in str(request.fspath):
        return

    from pyetm.config import curve_registry
    from pyetm.services.curve_metadata_service import CurveMetadataService

    # Derive curves from the registry so the mock can never drift from the canonical names.
    test_curves = curve_registry.default_curve_metadata()

    test_exports = [
        {"name": "energy_flow", "description": "Test export"},
        {"name": "energy_flow_present", "description": "Test export"},
        {"name": "molecule_flow", "description": "Test export"},
        {"name": "sankey", "description": "Test export"},
        {"name": "storage_parameters", "description": "Test export"},
        {"name": "costs_parameters", "description": "Test export"},
        {"name": "electricity_capacities", "description": "Test export"},
        {"name": "district_heating_capacities", "description": "Test export"},
        {"name": "hydrogen_capacities", "description": "Test export"},
        {"name": "network_gas_capacities", "description": "Test export"},
    ]

    # Mock the methods to return test data
    monkeypatch.setattr(
        CurveMetadataService,
        "get_curve_metadata",
        lambda: test_curves
    )
    monkeypatch.setattr(
        CurveMetadataService,
        "get_export_metadata",
        lambda: test_exports
    )
    monkeypatch.setattr(
        CurveMetadataService,
        "get_curve_names",
        lambda: [c["name"] for c in test_curves]
    )
    monkeypatch.setattr(
        CurveMetadataService,
        "get_export_names",
        lambda: [e["name"] for e in test_exports]
    )
    monkeypatch.setattr(
        CurveMetadataService,
        "get_curve_type",
        lambda curve_name: next((c["type"] for c in test_curves if c["name"] == curve_name), None)
    )


# Lazy‐import BaseClient
@pytest.fixture
def client():
    from pyetm.clients.base_client import BaseClient

    # Create a new client instance for each test (not using get_client())
    return BaseClient()


# Lazy‐import Scenario
@pytest.fixture
def scenario():
    from pyetm.models import Session

    return Session(id=999)


# --- Config Fixtures for Testing Optional Token and Custom Environments --- #


@pytest.fixture
def clean_settings_env(monkeypatch, tmp_path):
    """Create a completely clean environment for settings tests"""
    from pyetm.config.settings import reload_configuration

    # Clear caches to ensure each test gets a fresh config
    reload_configuration()

    # Clear all PYETM environment variables (with PYETM_ prefix)
    etm_vars = [
        "PYETM_ETM_API_TOKEN",
        "PYETM_BASE_URL",
        "PYETM_LOG_LEVEL",
        "PYETM_ENVIRONMENT",
        "PYETM_CSV_SEPARATOR",
        "PYETM_DECIMAL_SEPARATOR",
        "PYETM_SSL_VERIFY",
        "PYETM_TRUST_ENV",
        "PYETM_SSL_CERT_PATH",
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
    monkeypatch.setenv("ENVIRONMENT", "pro")

    # Explicitly set etm_api_token to None to override .env file
    return AppConfig(etm_api_token=None)


@pytest.fixture
def custom_env_config(monkeypatch):
    """Fixture for testing custom environment configuration (e.g., tyndp2024)"""
    from pyetm.config.settings import AppConfig, get_settings

    # Clear cache to ensure fresh config with test settings
    get_settings.cache_clear()

    monkeypatch.setenv("ETM_API_TOKEN", "etm_custom_token_for_tyndp")
    monkeypatch.setenv("ENVIRONMENT", "tyndp2024")

    return AppConfig()


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
