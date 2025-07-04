"""
Centralized fixtures for service tests. They will automatically be included.
"""

import pytest
from datetime import datetime
from pathlib import Path


@pytest.fixture
def api_url():
    """Base API URL for testing"""
    return "https://api.energytransitionmodel.com"


@pytest.fixture
def fake_response():
    """Factory fixture for creating fake HTTP responses"""

    class FakeResponse:
        def __init__(self, ok, status_code, json_data=None, text=""):
            self.ok = ok
            self.status_code = status_code
            self._json_data = json_data or {}
            self.text = text

        def json(self):
            return self._json_data

    return FakeResponse


@pytest.fixture
def dummy_client(fake_response):
    """Factory fixture for creating dummy API clients"""
    from types import SimpleNamespace

    class DummyClient:
        def __init__(self, response):
            self._response = response
            self.calls = []

        @property
        def session(self):
            return SimpleNamespace(get=self._mock_get)

        def _mock_get(self, url, params=None):
            self.calls.append((url, params))
            if isinstance(self._response, Exception):
                raise self._response
            return self._response

    def _make_client(response):
        if isinstance(response, dict):
            # If dict is provided, create a successful response
            return DummyClient(
                fake_response(ok=True, status_code=200, json_data=response)
            )
        return DummyClient(response)

    return _make_client


@pytest.fixture
def client(dummy_client):
    """A basic client instance for testing"""
    return dummy_client({})


@pytest.fixture
def dummy_scenario():
    """Factory fixture for creating dummy scenarios for service tests"""

    class DummyScenario:
        def __init__(self, scenario_id):
            self.id = scenario_id

    return DummyScenario


# --- Custom Curves Fixtures --- #


@pytest.fixture
def custom_curves_json():
    """JSON data for custom curves"""
    return [
        {
            "attached": True,
            "key": "interconnector_2_export_availability",
            "type": "availability",
        },
        {"attached": True, "key": "solar_pv_profile_1", "type": "profile"},
        {"attached": False, "key": "wind_profile_1", "type": "profile"},
    ]
