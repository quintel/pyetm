from pyetm.services.scenario_runners.destroy_session import (
    DestroySessionRunner,
)


def test_destroy_session_success(dummy_client, fake_response):
    """Test successful destruction of a Session."""
    body = {"message": "Session deleted successfully"}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="delete")

    result = DestroySessionRunner.run(client, scenario_id=456)
    assert result.success is True
    assert result.data == body
    assert result.errors == []
    assert client.calls == [("/scenarios/456", None)]


def test_destroy_session_http_failure_404(dummy_client, fake_response):
    """Test handling of 404 not found error."""
    response = fake_response(ok=False, status_code=404, text="Session not found")
    client = dummy_client(response, method="delete")

    result = DestroySessionRunner.run(client, scenario_id=99999)
    assert result.success is False
    assert result.data is None
    assert result.errors == ["404: Session not found"]


def test_destroy_session_http_failure_401(dummy_client, fake_response):
    """Test handling of 401 unauthorized error."""
    response = fake_response(ok=False, status_code=401, text="Unauthorized")
    client = dummy_client(response, method="delete")

    result = DestroySessionRunner.run(client, scenario_id=456)
    assert result.success is False
    assert result.data is None
    assert result.errors == ["401: Unauthorized"]


def test_destroy_session_http_failure_403(dummy_client, fake_response):
    """Test handling of 403 forbidden error (not owner)."""
    response = fake_response(
        ok=False, status_code=403, text="Not authorized to delete this session"
    )
    client = dummy_client(response, method="delete")

    result = DestroySessionRunner.run(client, scenario_id=456)
    assert result.success is False
    assert result.data is None
    assert result.errors == ["403: Not authorized to delete this session"]


def test_destroy_session_invalid_id_negative(dummy_client):
    """Test that negative IDs are rejected."""
    client = dummy_client({}, method="delete")

    result = DestroySessionRunner.run(client, scenario_id=-1)
    assert result.success is False
    assert result.data is None
    assert "Invalid scenario_id" in result.errors[0]
    assert len(client.calls) == 0


def test_destroy_session_invalid_id_zero(dummy_client):
    """Test that zero ID is rejected."""
    client = dummy_client({}, method="delete")

    result = DestroySessionRunner.run(client, scenario_id=0)
    assert result.success is False
    assert result.data is None
    assert "Invalid scenario_id" in result.errors[0]
    assert len(client.calls) == 0


def test_destroy_session_connection_error(dummy_client):
    """Test handling of connection errors."""
    client = dummy_client(ConnectionError("Connection failed"), method="delete")

    result = DestroySessionRunner.run(client, scenario_id=456)
    assert result.success is False
    assert result.data is None
    assert any("Connection failed" in err for err in result.errors)


def test_destroy_session_with_kwargs(dummy_client, fake_response):
    """Test that kwargs are passed through to the request."""
    body = {"message": "Deleted"}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response, method="delete")

    result = DestroySessionRunner.run(
        client, scenario_id=456, timeout=30
    )
    assert result.success is True
    assert result.data == body
    assert len(client.calls) == 1
    assert client.calls[0][0] == "/scenarios/456"


def test_destroy_session_build_request():
    """Test the build_request static method."""
    request = DestroySessionRunner.build_request(scenario_id=456)
    assert request["method"] == "delete"
    assert request["path"] == "/scenarios/456"
    assert request["payload"] is None
    assert request["kwargs"] == {}
