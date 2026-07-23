"""Tests for the CurveMetadataService.

The service fetches metadata through the shared ETM client (``get_client()``),
so these tests stub that client's session rather than mocking ``requests``.
"""

import json

import pytest

from pyetm.services.curve_metadata_service import (
    CurveMetadataService,
    _DEFAULT_CURVES,
    _DEFAULT_EXPORTS,
)

CURVES_PATH = "/curves/metadata"
EXPORTS_PATH = "/exports/metadata"


class _FakeResponse:
    """Minimal stand-in for ETMResponse."""

    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


class _FakeSession:
    """Stand-in for ETMSession.get used by CurveMetadataService."""

    def __init__(self):
        self._responses = {}
        self._error = None
        self.calls = 0

    def set_responses(self, responses):
        self._responses = responses
        self._error = None

    def set_error(self, error):
        self._error = error

    def get(self, path, **kwargs):
        self.calls += 1
        if self._error is not None:
            raise self._error
        if path not in self._responses:
            raise ConnectionError(f"unexpected metadata path: {path}")
        return _FakeResponse(self._responses[path])


class _FakeClient:
    def __init__(self, session):
        self.session = session


@pytest.fixture(autouse=True)
def service(tmp_path, monkeypatch):
    """Isolate the cache and route fetches to a controllable fake session."""
    CurveMetadataService.clear_cache()
    CurveMetadataService.set_cache_dir(tmp_path)

    session = _FakeSession()
    monkeypatch.setattr("pyetm.clients.get_client", lambda: _FakeClient(session))

    yield session

    CurveMetadataService.clear_cache()
    CurveMetadataService.clear_disk_cache()


class TestCurveMetadataService:
    """Tests for CurveMetadataService class."""

    def test_get_curve_metadata_from_api(self, service):
        service.set_responses(
            {
                CURVES_PATH: {
                    "hourly_outputs": [
                        {
                            "name": "electricity_profiles",
                            "type": "merit_curve",
                            "description": "Test description",
                        }
                    ]
                }
            }
        )

        metadata = CurveMetadataService.get_curve_metadata()

        assert len(metadata) == 1
        assert metadata[0]["name"] == "electricity_profiles"
        assert metadata[0]["type"] == "merit_curve"

    def test_get_curve_metadata_caches_result(self, service):
        service.set_responses(
            {CURVES_PATH: {"hourly_outputs": [{"name": "test", "type": "t", "description": "d"}]}}
        )

        CurveMetadataService.get_curve_metadata()
        CurveMetadataService.get_curve_metadata()

        assert service.calls == 1

    def test_in_memory_cache_expires_after_ttl(self, service):
        service.set_responses(
            {CURVES_PATH: {"hourly_outputs": [{"name": "x", "type": "t", "description": "d"}]}}
        )

        CurveMetadataService.get_curve_metadata()
        assert service.calls == 1

        # Simulate the cached entry being older than the TTL.
        CurveMetadataService._fetched_at["curves"] -= 10_000

        CurveMetadataService.get_curve_metadata()
        assert service.calls == 2

    def test_get_curve_metadata_saves_to_disk(self, service):
        service.set_responses(
            {CURVES_PATH: {"hourly_outputs": [{"name": "test", "type": "t", "description": "d"}]}}
        )

        CurveMetadataService.get_curve_metadata()

        cache_file = CurveMetadataService._cache_path("curves")
        assert cache_file.exists()

        payload = json.loads(cache_file.read_text(encoding="utf-8"))
        assert payload["data"][0]["name"] == "test"
        assert "fetched_at" in payload

    def test_uses_disk_cache_on_api_failure(self, service):
        service.set_responses(
            {CURVES_PATH: {"hourly_outputs": [{"name": "cached_curve", "type": "t", "description": "d"}]}}
        )
        CurveMetadataService.get_curve_metadata()

        CurveMetadataService.clear_cache()
        service.set_error(ConnectionError("server down"))

        metadata = CurveMetadataService.get_curve_metadata()

        assert len(metadata) == 1
        assert metadata[0]["name"] == "cached_curve"

    def test_falls_back_to_defaults_when_no_cache(self, service):
        service.set_error(ConnectionError("offline"))

        metadata = CurveMetadataService.get_curve_metadata()

        assert [c["name"] for c in metadata] == [c["name"] for c in _DEFAULT_CURVES]

    def test_falls_back_to_defaults_against_old_engine_404(self, service):
        # The shared session raises ValueError for 4xx responses; an engine
        # without the metadata endpoint (404) must degrade to defaults, not crash.
        service.set_error(ValueError("404: Not Found"))

        curve_names = CurveMetadataService.get_curve_names()
        export_names = CurveMetadataService.get_export_names()

        assert "electricity_profiles" in curve_names
        # Capacities are annual exports, not hourly curves.
        assert "electricity_capacities" not in curve_names
        assert "electricity_capacities" in export_names

    def test_get_export_metadata_from_api(self, service):
        service.set_responses(
            {EXPORTS_PATH: {"annual_exports": [{"name": "energy_flow", "description": "Test"}]}}
        )

        metadata = CurveMetadataService.get_export_metadata()

        assert len(metadata) == 1
        assert metadata[0]["name"] == "energy_flow"

    def test_get_export_metadata_caches_result(self, service):
        service.set_responses(
            {EXPORTS_PATH: {"annual_exports": [{"name": "test", "description": "d"}]}}
        )

        CurveMetadataService.get_export_metadata()
        CurveMetadataService.get_export_metadata()

        assert service.calls == 1

    def test_export_metadata_uses_disk_cache_on_api_failure(self, service):
        service.set_responses(
            {EXPORTS_PATH: {"annual_exports": [{"name": "cached_export", "description": "d"}]}}
        )
        CurveMetadataService.get_export_metadata()

        CurveMetadataService.clear_cache()
        service.set_error(ConnectionError("server down"))

        metadata = CurveMetadataService.get_export_metadata()

        assert metadata[0]["name"] == "cached_export"

    def test_export_metadata_falls_back_to_defaults(self, service):
        service.set_error(ConnectionError("offline"))

        metadata = CurveMetadataService.get_export_metadata()

        assert [e["name"] for e in metadata] == [e["name"] for e in _DEFAULT_EXPORTS]

    def test_get_curve_names(self, service):
        service.set_responses(
            {
                CURVES_PATH: {
                    "hourly_outputs": [
                        {"name": "curve1", "type": "t1", "description": "d1"},
                        {"name": "curve2", "type": "t2", "description": "d2"},
                    ]
                }
            }
        )

        assert CurveMetadataService.get_curve_names() == ["curve1", "curve2"]

    def test_get_export_names(self, service):
        service.set_responses(
            {
                EXPORTS_PATH: {
                    "annual_exports": [
                        {"name": "export1", "description": "d1"},
                        {"name": "export2", "description": "d2"},
                    ]
                }
            }
        )

        assert CurveMetadataService.get_export_names() == ["export1", "export2"]

    def test_get_curve_type(self, service):
        service.set_responses(
            {
                CURVES_PATH: {
                    "hourly_outputs": [
                        {"name": "electricity_profiles", "type": "merit_curve", "description": "d"}
                    ]
                }
            }
        )

        assert CurveMetadataService.get_curve_type("electricity_profiles") == "merit_curve"

    def test_get_curve_type_not_found(self, service):
        service.set_responses({CURVES_PATH: {"hourly_outputs": []}})

        assert CurveMetadataService.get_curve_type("unknown_curve") is None

    def test_clear_cache_forces_refetch(self, service):
        service.set_responses(
            {CURVES_PATH: {"hourly_outputs": [{"name": "test", "type": "t", "description": "d"}]}}
        )

        CurveMetadataService.get_curve_metadata()
        CurveMetadataService.clear_cache()
        CurveMetadataService.get_curve_metadata()

        assert service.calls == 2

    def test_clear_disk_cache(self, service):
        service.set_responses(
            {CURVES_PATH: {"hourly_outputs": [{"name": "test", "type": "t", "description": "d"}]}}
        )
        CurveMetadataService.get_curve_metadata()

        cache_file = CurveMetadataService._cache_path("curves")
        assert cache_file.exists()

        CurveMetadataService.clear_disk_cache()
        assert not cache_file.exists()


class TestCacheNamespacing:
    """The cache must not be shared across environments (base URLs)."""

    def test_cache_path_namespaced_by_base_url(self, monkeypatch):
        class _Settings:
            base_url = "https://a.example.com/api/v3"
            metadata_timeout = 5.0
            metadata_cache_ttl = 3600.0

        class _OtherSettings(_Settings):
            base_url = "https://b.example.com/api/v3"

        monkeypatch.setattr(
            "pyetm.services.curve_metadata_service.get_settings", lambda: _Settings()
        )
        path_a = CurveMetadataService._cache_path("curves")

        monkeypatch.setattr(
            "pyetm.services.curve_metadata_service.get_settings", lambda: _OtherSettings()
        )
        path_b = CurveMetadataService._cache_path("curves")

        assert path_a.name.startswith("curves_metadata_")
        assert path_a != path_b

    def test_reload_configuration_clears_in_memory_cache(self, service):
        service.set_responses(
            {CURVES_PATH: {"hourly_outputs": [{"name": "x", "type": "t", "description": "d"}]}}
        )
        CurveMetadataService.get_curve_metadata()
        assert CurveMetadataService._cache

        from pyetm.config.settings import reload_configuration

        reload_configuration()

        assert CurveMetadataService._cache == {}
