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
def _mount_requests_mock(requests_mock, client):
    """
    requests_mock._adapter is the HTTPAdapter instance used
    by the pytest-requests-mock plugin.
    """
    adapter = getattr(requests_mock, "_adapter", None)
    if adapter and hasattr(client, "session") and hasattr(client.session, "session"):
        client.session.session.mount("http://", adapter)
        client.session.session.mount("https://", adapter)


# Lazy‐import BaseClient
@pytest.fixture
def client():
    from pyetm.clients.base_client import BaseClient

    return BaseClient()


# Lazy‐import Scenario
@pytest.fixture
def scenario():
    from pyetm.models import Scenario

    return Scenario(id=999)


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
