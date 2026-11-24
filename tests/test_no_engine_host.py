"""Every request pyETM makes goes to MyETM's API, whatever the Scenario does."""

import json
from contextlib import asynccontextmanager

from pyetm.clients.base_client import BaseClient
from pyetm.models import Scenario

MYETM = "https://my.energytransitionmodel.com/api/v4"
SAVED = {"id": 7, "title": "T", "scenario_id": 70, "area_code": "nl2023", "end_year": 2050, "version": "latest"}


class FakeResponse:
    def __init__(self, url, body, content_type="application/json"):
        self.status = 200
        self.url = url
        self.headers = {"content-type": content_type}
        self._body = body

    async def json(self):
        return json.loads(self._body)

    async def text(self):
        return self._body

    async def read(self):
        return self._body.encode()


def body_for(url):
    """Answer each path with a plausible body, as MyETM or the relayed engine would."""
    if url.endswith(".csv"):
        return "\n".join(["1.0"] * 8760), "text/csv"
    if "/users" in url:
        return json.dumps({"data": [{"email": "a@example.com", "role": "scenario_owner", "pending": False}]}), "application/json"
    if url.endswith("/inputs"):
        return json.dumps({"x": {"min": 0, "max": 10, "default": 1, "unit": "%"}}), "application/json"
    if url.endswith("/7/") or url.endswith("/8/"):
        return json.dumps({"id": 70, "start_year": 2023, "template": None}), "application/json"
    return json.dumps({"data": {**SAVED, "id": 8 if "saved_scenarios" in url and url.endswith("saved_scenarios") else 7}}), "application/json"


class RecordingSession:
    def __init__(self):
        self.urls = []

    async def close(self):
        pass

    @asynccontextmanager
    async def request(self, method, url, **kwargs):
        self.urls.append(url)
        body, content_type = body_for(url)
        yield FakeResponse(url, body, content_type)


def test_no_request_reaches_an_engine_host(monkeypatch):
    client = BaseClient()
    recorder = RecordingSession()
    client.session._session = recorder
    monkeypatch.setattr(client.session, "_ensure_session", lambda: None)

    try:
        scenario = Scenario.load(7, client=client)
        scenario.inputs
        scenario.start_year
        scenario.update_user_values({"x": 5})
        scenario.get_hourly_curve("electricity_profiles")
        scenario.list_users()
        scenario.save()
        scenario.copy(title="Copy")
    finally:
        client.close()

    assert len(recorder.urls) >= 8
    assert all(url.startswith(MYETM) for url in recorder.urls), recorder.urls
    assert not any("engine" in url for url in recorder.urls)
