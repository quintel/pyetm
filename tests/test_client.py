import pytest

from pyETM.clients.session import RequestsSession, AuthenticationError, GenericError

BASE_URL = "https://example.com/api"

def test_requests_session_401_raises_auth(requests_mock):
    """ if the API returns 401 we must get AuthenticationError """
    # mock GET https://example.com/api/whatever => 401
    requests_mock.get(f"{BASE_URL}/foo", status_code=401, text="nope")
    sess = RequestsSession()
    with pytest.raises(AuthenticationError):
        sess.get("/foo")

def test_requests_session_500_raises_etm_error(requests_mock):
    """ any 4xx/5xx aside from 401 becomes GenericError for now """
    requests_mock.post(f"{BASE_URL}/bar", status_code=500, text="boom")
    sess = RequestsSession()
    with pytest.raises(GenericError) as exc:
        sess.post("/bar", json={})
    assert "HTTP 500" in str(exc.value)
