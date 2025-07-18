import pytest


@pytest.fixture
def api_url():
    """Base API URL for testing"""
    return "https://engine.energytransitionmodel.com"


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
        def __init__(self, response, supported_methods=None):
            self._response = response
            self.calls = []
            self.supported_methods = supported_methods or ["get"]

        @property
        def session(self):
            # Create a session with all supported HTTP methods
            session_methods = {}
            for method in self.supported_methods:
                session_methods[method] = self._create_mock_method(method)
            return SimpleNamespace(**session_methods)

        def _create_mock_method(self, method):
            def mock_method(url, params=None, json=None, **kwargs):
                # Record the call with all parameters
                call_data = {}
                if params is not None:
                    call_data["params"] = params
                if json is not None:
                    call_data["json"] = json
                if kwargs:
                    call_data.update(kwargs)

                # If no parameters, record None for backwards compatibility
                call_record = (url, call_data if call_data else None)
                self.calls.append(call_record)

                if isinstance(self._response, Exception):
                    raise self._response
                return self._response

            return mock_method

    def _make_client(response, method="get"):
        if isinstance(response, dict):
            # If dict is provided, create a successful response
            return DummyClient(
                fake_response(ok=True, status_code=200, json_data=response),
                supported_methods=[method],
            )
        return DummyClient(response, supported_methods=[method])

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
