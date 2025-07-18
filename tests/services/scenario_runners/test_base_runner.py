from unittest.mock import Mock
from pyetm.services.scenario_runners.base_runner import BaseRunner
from pyetm.services.service_result import ServiceResult
from pyetm.clients.base_client import BaseClient


class RealRunner(BaseRunner):
    @staticmethod
    def run(client: BaseClient, scenario, **kwargs):
        return ServiceResult.ok(data="test_result")


def test_make_request_get_with_json_response():
    """Test GET request with successful JSON response"""
    mock_client = Mock(spec=BaseClient)
    mock_client.session = Mock()
    mock_response = Mock()
    mock_response.ok = True
    mock_response.json.return_value = {"result": "success"}
    mock_client.session.get.return_value = mock_response

    result = RealRunner._make_request(mock_client, "GET", "/test-path")

    assert result.success
    assert result.data == {"result": "success"}
    mock_client.session.get.assert_called_once_with("/test-path")


def test_make_request_get_with_payload_as_params():
    """Test GET request with payload converted to params"""
    mock_client = Mock(spec=BaseClient)
    mock_client.session = Mock()
    mock_response = Mock()
    mock_response.ok = True
    mock_response.json.return_value = {"result": "success"}
    mock_client.session.get.return_value = mock_response

    payload = {"param1": "value1", "param2": "value2"}
    result = RealRunner._make_request(mock_client, "GET", "/test-path", payload=payload)

    assert result.success
    assert result.data == {"result": "success"}
    mock_client.session.get.assert_called_once_with("/test-path", params=payload)


def test_make_request_put_with_json_payload():
    """Test PUT request with payload as JSON body"""
    mock_client = Mock(spec=BaseClient)
    mock_client.session = Mock()
    mock_response = Mock()
    mock_response.ok = True
    mock_response.json.return_value = {"updated": "success"}
    mock_client.session.put.return_value = mock_response

    payload = {"data": "update_value"}
    result = RealRunner._make_request(mock_client, "PUT", "/test-path", payload=payload)

    assert result.success
    assert result.data == {"updated": "success"}
    mock_client.session.put.assert_called_once_with("/test-path", json=payload)


def test_make_request_post_with_json_payload():
    """Test POST request with payload as JSON body"""
    mock_client = Mock(spec=BaseClient)
    mock_client.session = Mock()
    mock_response = Mock()
    mock_response.ok = True
    mock_response.json.return_value = {"created": "success"}
    mock_client.session.post.return_value = mock_response

    payload = {"data": "create_value"}
    result = RealRunner._make_request(
        mock_client, "POST", "/test-path", payload=payload
    )

    assert result.success
    assert result.data == {"created": "success"}
    mock_client.session.post.assert_called_once_with("/test-path", json=payload)


def test_make_request_patch_with_json_payload():
    """Test PATCH request with payload as JSON body"""
    mock_client = Mock(spec=BaseClient)
    mock_client.session = Mock()
    mock_response = Mock()
    mock_response.ok = True
    mock_response.json.return_value = {"patched": "success"}
    mock_client.session.patch.return_value = mock_response

    payload = {"data": "patch_value"}
    result = RealRunner._make_request(
        mock_client, "PATCH", "/test-path", payload=payload
    )

    assert result.success
    assert result.data == {"patched": "success"}
    mock_client.session.patch.assert_called_once_with("/test-path", json=payload)


def test_make_request_with_kwargs():
    """Test request with additional kwargs"""
    mock_client = Mock(spec=BaseClient)
    mock_client.session = Mock()
    mock_response = Mock()
    mock_response.ok = True
    mock_response.json.return_value = {"result": "success"}
    mock_client.session.get.return_value = mock_response

    result = RealRunner._make_request(
        mock_client, "GET", "/test-path", timeout=30, headers={"Custom": "header"}
    )

    assert result.success
    assert result.data == {"result": "success"}
    mock_client.session.get.assert_called_once_with(
        "/test-path", timeout=30, headers={"Custom": "header"}
    )


def test_make_request_with_payload_and_kwargs():
    """Test PUT request with both payload and additional kwargs"""
    mock_client = Mock(spec=BaseClient)
    mock_client.session = Mock()
    mock_response = Mock()
    mock_response.ok = True
    mock_response.json.return_value = {"result": "success"}
    mock_client.session.put.return_value = mock_response

    payload = {"data": "value"}
    result = RealRunner._make_request(
        mock_client, "PUT", "/test-path", payload=payload, timeout=30
    )

    assert result.success
    assert result.data == {"result": "success"}
    mock_client.session.put.assert_called_once_with(
        "/test-path", json=payload, timeout=30
    )


def test_make_request_non_json_response():
    """Test request with non-JSON response"""
    mock_client = Mock(spec=BaseClient)
    mock_client.session = Mock()
    mock_response = Mock()
    mock_response.ok = True
    mock_response.json.side_effect = ValueError("Not valid JSON")
    mock_client.session.get.return_value = mock_response

    result = RealRunner._make_request(mock_client, "GET", "/test-path")

    assert result.success
    assert result.data == mock_response


def test_make_request_http_error():
    """Test HTTP error response"""
    mock_client = Mock(spec=BaseClient)
    mock_client.session = Mock()
    mock_response = Mock()
    mock_response.ok = False
    mock_response.status_code = 404
    mock_response.text = "Not Found"
    mock_client.session.get.return_value = mock_response

    result = RealRunner._make_request(mock_client, "GET", "/test-path")

    assert not result.success
    assert result.data is None
    assert result.errors == ["404: Not Found"]


def test_make_request_connection_error():
    """Test connection error handling"""
    mock_client = Mock(spec=BaseClient)
    mock_client.session = Mock()
    mock_client.session.get.side_effect = ConnectionError("Connection failed")

    result = RealRunner._make_request(mock_client, "GET", "/test-path")

    assert not result.success
    assert result.data is None
    assert result.errors == ["Connection failed"]


def test_make_request_permission_error():
    """Test permission error handling"""
    mock_client = Mock(spec=BaseClient)
    mock_client.session = Mock()
    mock_client.session.get.side_effect = PermissionError("Access denied")

    result = RealRunner._make_request(mock_client, "GET", "/test-path")

    assert not result.success
    assert result.data is None
    assert result.errors == ["Access denied"]


def test_make_request_value_error():
    """Test value error handling"""
    mock_client = Mock(spec=BaseClient)
    mock_client.session = Mock()
    mock_client.session.get.side_effect = ValueError("Invalid value")

    result = RealRunner._make_request(mock_client, "GET", "/test-path")

    assert not result.success
    assert result.data is None
    assert result.errors == ["Invalid value"]


def test_make_request_generic_exception():
    """Test generic exception handling"""
    mock_client = Mock(spec=BaseClient)
    mock_client.session = Mock()
    mock_client.session.get.side_effect = RuntimeError("Unexpected error")

    result = RealRunner._make_request(mock_client, "GET", "/test-path")

    assert not result.success
    assert result.data is None
    assert result.errors == ["Unexpected error"]


def test_concrete_runner_implementation():
    """Test that concrete runner implementations work"""
    mock_client = Mock(spec=BaseClient)
    mock_scenario = {"test": "scenario"}

    result = RealRunner.run(mock_client, mock_scenario)

    assert result.success
    assert result.data == "test_result"
