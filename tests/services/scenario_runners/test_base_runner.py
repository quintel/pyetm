from unittest.mock import Mock
from pyetm.services.scenario_runners.base_runner import BaseRunner
from pyetm.services.service_result import ServiceResult
from pyetm.clients.base_client import BaseClient


class RealRunner(BaseRunner):
    @staticmethod
    def run(client: BaseClient, scenario, **kwargs):
        return ServiceResult.ok(data="test_result")


def test_make_request_non_json_response():
    mock_client = Mock(spec=BaseClient)
    mock_client.session = Mock()
    mock_response = Mock()
    mock_response.ok = True
    mock_response.json.side_effect = ValueError("Not valid JSON")
    mock_client.session.get.return_value = mock_response

    result = RealRunner._make_request(mock_client, "GET", "/test-path")

    assert result.success
    assert result.data == mock_response


def test_concrete_runner_implementation():
    mock_client = Mock(spec=BaseClient)
    mock_scenario = {"test": "scenario"}

    result = RealRunner.run(mock_client, mock_scenario)

    assert result.success
    assert result.data == "test_result"
