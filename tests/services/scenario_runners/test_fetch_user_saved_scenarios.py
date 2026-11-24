from types import SimpleNamespace

from pyetm.services.scenario_runners.fetch_user_saved_scenarios import FetchUserSavedScenariosRunner


def paged_client(fake_response, pages):
    """A client answering each GET with the next page, recording the query it was sent."""
    calls = []
    bodies = iter(pages)

    def get(url, params=None, **kwargs):
        calls.append((url, params))
        return fake_response(True, 200, next(bodies))

    return SimpleNamespace(session=SimpleNamespace(get=get)), calls


def page(items, number, pages):
    return {"data": items, "meta": {"pagination": {"page": number, "limit": 100, "pages": pages, "count": 3}}}


def test_follows_every_page(fake_response):
    client, calls = paged_client(fake_response, [page([{"id": 1}, {"id": 2}], 1, 2), page([{"id": 3}], 2, 2)])

    result = FetchUserSavedScenariosRunner.run(client)

    assert [item["id"] for item in result.data] == [1, 2, 3]
    assert calls == [
        ("/saved_scenarios", {"page": 1, "limit": 100}),
        ("/saved_scenarios", {"page": 2, "limit": 100}),
    ]


def test_an_empty_index_is_one_page(fake_response):
    client, calls = paged_client(fake_response, [page([], 1, 1)])

    assert FetchUserSavedScenariosRunner.run(client).data == []
    assert len(calls) == 1


def test_fails_when_a_page_fails(dummy_client):
    client = dummy_client(PermissionError("401: unauthenticated"))

    assert not FetchUserSavedScenariosRunner.run(client).success
