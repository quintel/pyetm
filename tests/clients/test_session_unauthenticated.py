"""Tests for unauthenticated (no token) ETMSession operations."""

import pytest
from unittest.mock import patch
from pyetm.clients.session import ETMSession


def test_session_headers_without_token(unauthenticated_config):
    """Test that Authorization header is not set when no token is provided."""
    from pyetm.config.settings import get_settings

    # Ensure settings cache is cleared
    get_settings.cache_clear()

    # Mock get_settings to return config with no token
    # This prevents the .env file from being loaded
    with patch('pyetm.clients.session.get_settings') as mock_get_settings:
        mock_get_settings.return_value = unauthenticated_config
        session = ETMSession()

    # Verify token is None
    assert session.token is None

    # Verify Authorization header is not present
    assert "Authorization" not in session.headers

    # Verify Accept header is still present
    assert session.headers["Accept"] == "application/json"


def test_session_headers_with_token(monkeypatch):
    """Test that Authorization header is set when token is provided."""
    from pyetm.config.settings import get_settings

    # Clear cache and set token
    get_settings.cache_clear()
    monkeypatch.setenv("ETM_API_TOKEN", "etm_test_token_123")

    session = ETMSession()

    # Verify token is set
    assert session.token == "etm_test_token_123"

    # Verify Authorization header is present
    assert "Authorization" in session.headers
    assert session.headers["Authorization"] == "Bearer etm_test_token_123"

    # Verify Accept header is still present
    assert session.headers["Accept"] == "application/json"


def test_response_401_error_without_token():
    """Test that 401 error without token provides helpful message."""
    from pyetm.clients.session import ETMResponse

    response = ETMResponse(
        headers={},
        url="https://engine.energytransitionmodel.com/api/v3/scenarios",
        text="Unauthorized",
        status_code=401,
    )

    with pytest.raises(PermissionError) as exc_info:
        response.raise_for_status(token=None)

    error_msg = str(exc_info.value)
    assert "Authentication required" in error_msg
    assert "ETM_API_TOKEN" in error_msg
    assert "energytransitionmodel.com/api_access" in error_msg


def test_response_401_error_with_invalid_token():
    """Test that 401 error with token indicates invalid token."""
    from pyetm.clients.session import ETMResponse

    response = ETMResponse(
        headers={},
        url="https://engine.energytransitionmodel.com/api/v3/scenarios",
        text="Unauthorized",
        status_code=401,
    )

    with pytest.raises(PermissionError) as exc_info:
        response.raise_for_status(token="etm_invalid_token")

    error_msg = str(exc_info.value)
    assert "Invalid ETM_API_TOKEN" in error_msg
    assert "Authentication required" not in error_msg


def test_response_200_success_without_token():
    """Test that successful responses work without token."""
    from pyetm.clients.session import ETMResponse

    response = ETMResponse(
        headers={},
        url="https://engine.energytransitionmodel.com/api/v3/scenarios/123",
        text='{"id": 123, "title": "Test Scenario"}',
        status_code=200,
    )

    # Should not raise any exception
    response.raise_for_status(token=None)

    # Should be able to parse JSON
    data = response.json()
    assert data["id"] == 123
    assert data["title"] == "Test Scenario"
