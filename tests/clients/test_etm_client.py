"""Tests for ETMClient's headers, its shared event loop, and ETMResponse's errors."""

import asyncio
import os
import signal
import threading

import pytest
from aiohttp import web
from unittest.mock import patch

from pyetm.clients.base_client import AsyncBatchRunner
from pyetm.clients.etm_client import USER_AGENT, ETMClient, ETMResponse, shared_loop


def response(status_code, text):
    return ETMResponse(headers={}, url="https://my.energytransitionmodel.com/api/v4", text=text, status_code=status_code)


def test_headers_without_a_token(unauthenticated_config):
    with patch("pyetm.clients.etm_client.get_settings") as get_settings:
        get_settings.return_value = unauthenticated_config
        client = ETMClient()

    try:
        assert client.token is None
        assert "Authorization" not in client.headers
        assert client.headers["Accept"] == "application/json"
    finally:
        client.close()


def test_headers_with_a_token(monkeypatch):
    from pyetm.config.settings import get_settings

    get_settings.cache_clear()
    monkeypatch.setenv("ETM_API_TOKEN", "etm_test_token_123")
    client = ETMClient()

    try:
        assert client.headers["Authorization"] == "Bearer etm_test_token_123"
    finally:
        client.close()


def test_every_request_names_pyetm_and_its_version():
    from importlib.metadata import version

    client = ETMClient()
    try:
        assert client.headers["User-Agent"] == USER_AGENT == f"pyetm/{version('pyetm')}"
    finally:
        client.close()


# Bodies captured from the local stack: MyETM's own, the relay's refusals, nginx's, and ETEngine's
# relayed verbatim through the passthrough.
@pytest.mark.parametrize(
    "status, body, exception, message",
    [
        (
            404,
            '{"errors":[{"status":404,"code":"scenario_not_found","detail":"Saved scenario not found"}]}',
            ValueError,
            "404: scenario_not_found: Saved scenario not found",
        ),
        (
            422,
            '{"errors":[{"status":422,"code":"validation_failed","detail":"must be an integer",'
            '"source":{"pointer":"/saved_scenario/end_year"}}]}',
            ValueError,
            "422: validation_failed: must be an integer (at /saved_scenario/end_year)",
        ),
        (
            403,
            '{"errors":[{"status":403,"code":"forbidden","detail":"Not relayed"}]}',
            PermissionError,
            "403: forbidden: Not relayed",
        ),
        (
            429,
            '{"errors":[{"status":429,"code":"too_many_requests","detail":"Too many concurrent calls"}]}',
            ValueError,
            "429: too_many_requests: Too many concurrent calls",
        ),
        (
            502,
            '{"errors":[{"status":502,"code":"engine_unavailable","detail":"The engine did not answer"}]}',
            ConnectionError,
            "502: engine_unavailable: The engine did not answer",
        ),
        (
            422,
            '{"errors":["Input no_such_input does not exist"]}',
            ValueError,
            "422: Input no_such_input does not exist",
        ),
        (
            422,
            '{"errors":{"area_code":["is unknown","is required"]}}',
            ValueError,
            "422: area_code: is unknown, area_code: is required",
        ),
        (400, '{"error":"Interpolated scenario must have an end year"}', ValueError, "400: Interpolated scenario must have an end year"),
        (504, "<html>Gateway Time-out</html>", ConnectionError, "504: <html>Gateway Time-out</html>"),
        (500, "", ConnectionError, "500"),
    ],
)
def test_an_error_body_renders_as_one_message(status, body, exception, message):
    with pytest.raises(exception) as error:
        response(status, body).raise_for_status(token="etm_token")

    assert str(error.value) == message


def test_401_says_no_token_is_set():
    body = '{"errors":[{"status":401,"code":"unauthenticated","detail":"Not authenticated"}]}'

    with pytest.raises(PermissionError) as error:
        response(401, body).raise_for_status(token=None)

    assert str(error.value).startswith("401: unauthenticated: Not authenticated. This needs an ETM_API_TOKEN")
    assert "none is set" in str(error.value)


def test_401_says_the_token_was_refused():
    body = '{"errors":[{"status":401,"code":"unauthenticated","detail":"Not authenticated"}]}'

    with pytest.raises(PermissionError) as error:
        response(401, body).raise_for_status(token="etm_expired")

    assert "Your ETM_API_TOKEN was refused" in str(error.value)


def test_a_success_does_not_raise():
    ok = response(200, '{"id": 123}')

    ok.raise_for_status(token=None)
    assert ok.json() == {"id": 123}


# ------ The shared event loop ------ #


@pytest.fixture
def ping_server():
    async def ping(request):
        return web.json_response({"ok": True})

    async def start():
        app = web.Application()
        app.router.add_get("/ping", ping)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        return runner, f"http://127.0.0.1:{runner.addresses[0][1]}"

    runner, url = asyncio.run_coroutine_threadsafe(start(), shared_loop()).result()
    yield url
    asyncio.run_coroutine_threadsafe(runner.cleanup(), shared_loop()).result()


def loop_threads():
    return [thread for thread in threading.enumerate() if thread.name == "pyetm-event-loop"]


def test_creating_clients_starts_no_thread():
    before = threading.active_count()

    clients = [ETMClient() for _ in range(3)]

    assert threading.active_count() == before
    for client in clients:
        client.close()


def test_clients_share_one_loop_and_closing_one_leaves_the_other_working(ping_server):
    first, second = ETMClient(base_url=ping_server), ETMClient(base_url=ping_server)
    try:
        assert first.get("/ping").json() == second.get("/ping").json() == {"ok": True}
        assert len(loop_threads()) == 1

        first.close()

        assert second.get("/ping").json() == {"ok": True}
        assert first.get("/ping").json() == {"ok": True}
    finally:
        first.close()
        second.close()


def test_a_batch_can_be_a_new_clients_first_call(ping_server):
    client = ETMClient(base_url=ping_server)
    try:
        results = AsyncBatchRunner.batch_requests_sync(client, [{"method": "GET", "url": "/ping"}] * 2)

        assert [(r.success, r.data) for r in results] == [(True, {"ok": True})] * 2
    finally:
        client.close()


@pytest.mark.skipif(not hasattr(os, "fork"), reason="fork is POSIX only")
@pytest.mark.filterwarnings("ignore:.*fork.*:DeprecationWarning")
def test_a_forked_child_runs_its_own_loop(ping_server):
    client = ETMClient(base_url=ping_server)
    client.get("/ping")
    read_end, write_end = os.pipe()

    pid = os.fork()
    if pid == 0:
        try:
            signal.alarm(5)
            os.write(write_end, client.get("/ping").text.encode())
        finally:
            os._exit(0)

    os.close(write_end)
    try:
        assert os.read(read_end, 100) == b'{"ok": true}'
    finally:
        os.waitpid(pid, 0)
        client.close()
