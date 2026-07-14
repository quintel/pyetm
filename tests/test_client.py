from pyetm.clients.base_client import BaseClient, get_client

BASE_URL = "https://example.com/api"


def test_base_client_creates_separate_instances():
    """Test that BaseClient() creates separate instances."""
    with BaseClient() as c1, BaseClient() as c2:
        assert c1 is not c2


def test_get_client_returns_cached_instance():
    """Test that get_client() returns the same cached instance."""
    c1 = get_client()
    c2 = get_client()
    assert c1 is c2


def test_multiple_clients_with_different_configs():
    """Test that we can create multiple clients with different configurations."""
    with BaseClient(
        base_url="https://engine.energytransitionmodel.com/api/v3"
    ) as prod_client, BaseClient(
        base_url="https://beta.engine.energytransitionmodel.com/api/v3"
    ) as beta_client:
        assert prod_client is not beta_client
        assert prod_client.session.base_url == "https://engine.energytransitionmodel.com/api/v3"
        assert beta_client.session.base_url == "https://beta.engine.energytransitionmodel.com/api/v3"
