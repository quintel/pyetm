import pytest
from pyetm.services.scenario_runners.fetch_metadata import FetchMetadataRunner


def test_fetch_metadata_success_full(dummy_client, fake_response, dummy_scenario):
    body = {k: f"value_{k}" for k in FetchMetadataRunner.META_KEYS}
    response = fake_response(ok=True, status_code=200, json_data=body)
    client = dummy_client(response)
    scenario = dummy_scenario(42)

    result = FetchMetadataRunner.run(client, scenario)
    assert result.success is True
    assert result.data == body
    assert result.errors == []
    assert client.calls == [("/scenarios/42", None)]


def test_fetch_metadata_missing_keys_warns(dummy_client, fake_response, dummy_scenario):
    response = fake_response(ok=True, status_code=200, json_data={})
    client = dummy_client(response)
    scenario = dummy_scenario(7)

    result = FetchMetadataRunner.run(client, scenario)
    assert result.success is True
    assert all(v is None for v in result.data.values())
    assert len(result.errors) == len(FetchMetadataRunner.META_KEYS)
    assert all("Missing field in response" in w for w in result.errors)


def test_fetch_metadata_http_failure(dummy_client, fake_response, dummy_scenario):
    response = fake_response(ok=False, status_code=404, text="Not Found")
    client = dummy_client(response)
    scenario = dummy_scenario(9)

    result = FetchMetadataRunner.run(client, scenario)
    assert result.success is False
    assert result.data is None
    assert result.errors == ["404: Not Found"]


def test_fetch_metadata_exception(dummy_client, dummy_scenario):
    client = dummy_client(ValueError("boom! something went wrong"))
    scenario = dummy_scenario(5)

    result = FetchMetadataRunner.run(client, scenario)
    assert result.success is False
    assert result.data is None
    assert any("boom! something went wrong" in err for err in result.errors)
