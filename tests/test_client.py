from pyetm.clients.base_client import BaseClient

BASE_URL = "https://example.com/api"

# TODO: Add test cases for 401 or 500 errors at least


def test_base_client_is_singleton():
    c1 = BaseClient()
    c2 = BaseClient()
    assert c1 is c2
