import pytest

from pyetm.services.scenario_runners.saved_scenario_users_create import SavedScenarioUsersCreateRunner
from pyetm.services.scenario_runners.saved_scenario_users_destroy import SavedScenarioUsersDestroyRunner
from pyetm.services.scenario_runners.saved_scenario_users_index import SavedScenarioUsersIndexRunner
from pyetm.services.scenario_runners.saved_scenario_users_update import SavedScenarioUsersUpdateRunner

OWNER = {"email": "a@example.com", "role": "scenario_owner", "pending": False}


def batch(*items):
    """A v4 batch body, which MyETM answers with 207 whatever happened."""
    return {"data": list(items), "meta": {"batch": {"total": len(items)}}}


def test_index_unwraps_the_envelope(dummy_client, fake_response):
    client = dummy_client(fake_response(True, 200, {"data": [OWNER], "meta": {}}))

    result = SavedScenarioUsersIndexRunner.run(client, 5)

    assert result.data == [OWNER]
    assert client.calls == [("/saved_scenarios/5/users", None)]


@pytest.mark.parametrize(
    "runner, method",
    [
        (SavedScenarioUsersCreateRunner, "post"),
        (SavedScenarioUsersUpdateRunner, "put"),
        (SavedScenarioUsersDestroyRunner, "delete"),
    ],
)
def test_a_batch_sends_its_users_and_returns_each_applied_one(runner, method, dummy_client, fake_response):
    client = dummy_client(fake_response(True, 207, batch({"status": "ok", "data": OWNER})), method=method)
    users = [{"email": "a@example.com", "role": "scenario_owner"}]

    result = runner.run(client, 5, users)

    assert result.data == [OWNER]
    assert client.calls == [("/saved_scenarios/5/users", {"json": {"saved_scenario_users": users}})]


def test_a_refused_item_fails_the_batch_naming_it(dummy_client, fake_response):
    refused = {"status": "error", "code": "validation_failed", "detail": "role is invalid", "source": {}}
    client = dummy_client(fake_response(True, 207, batch(refused)), method="post")

    result = SavedScenarioUsersCreateRunner.run(client, 5, [{"email": "a@example.com", "role": "boss"}])

    assert not result.success
    assert result.errors == ["validation_failed: role is invalid"]


def test_a_user_without_an_email_is_refused_before_sending(dummy_client):
    client = dummy_client({}, method="delete")

    result = SavedScenarioUsersDestroyRunner.run(client, 5, [{"role": "scenario_owner"}])

    assert result.errors == ["User 0: Missing required fields: email"]
    assert client.calls == []


def test_no_users_is_refused(dummy_client):
    assert SavedScenarioUsersUpdateRunner.run(dummy_client({}, method="put"), 5, []).errors == ["No users provided"]
