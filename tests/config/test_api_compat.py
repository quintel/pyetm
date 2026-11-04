"""Saved scenario bodies go under a root key and carry the environment they belong to."""

import pytest

from pyetm.config.api_compat import _environment_label, saved_scenario_payload
from pyetm.config.settings import _infer_base_url_from_env

ATTRIBUTES = {"scenario_id": 123, "title": "Fix check", "private": False}
PERMITTED = ("scenario_id", "title", "description", "private")

STABLE = "https://2026-01.engine.energytransitionmodel.com/api/v3"
PRO = "https://engine.energytransitionmodel.com/api/v3"


@pytest.mark.parametrize(
    "base_url",
    [
        PRO,
        STABLE,
        "https://beta.engine.energytransitionmodel.com/api/v3",
        "http://localhost:3000/api/v3",
        None,
    ],
)
def test_every_engine_receives_the_root_key(base_url):
    """params.require(:saved_scenario) reads the attributes from the root key."""
    nested = saved_scenario_payload(ATTRIBUTES, base_url)["saved_scenario"]

    assert {key: nested[key] for key in PERMITTED if key in nested} == ATTRIBUTES


@pytest.mark.parametrize(
    ("base_url", "label"),
    [
        (STABLE, "2026-01"),
        ("https://beta.engine.energytransitionmodel.com/api/v3", "beta"),
        ("https://tyndp2024.engine.energytransitionmodel.com/api/v3", "tyndp2024"),
        ("https://2026-s1.engine.energytransitionmodel.com/api/v3", "2026-s1"),
    ],
)
def test_environment_label_identifies_non_production_hosts(base_url, label):
    assert _environment_label(base_url) == label


@pytest.mark.parametrize("environment", ["2026-01", "beta", "tyndp2024", "2026-S1"])
def test_environment_label_round_trips_through_the_inferred_base_url(environment):
    """A label must survive settings' own env-to-URL inference."""
    base_url = _infer_base_url_from_env(environment)

    assert _environment_label(base_url) == environment.lower()


@pytest.mark.parametrize(
    "base_url",
    [PRO, "http://localhost:3000/api/v3", None, ""],
)
def test_environment_label_is_none_for_production_and_local(base_url):
    assert _environment_label(base_url) is None


def test_non_production_payload_is_stamped_with_the_environment_as_version():
    """MyETM defaults an unlabelled saved scenario to production.

    Without this, every non-production save silently strands the scenario on the
    wrong engine for MyETM's follow-up callbacks (role grants, protection, tagging).
    """
    body = saved_scenario_payload(ATTRIBUTES, STABLE)

    assert body["saved_scenario"]["version"] == "2026-01"


def test_production_payload_is_not_stamped_with_a_version():
    body = saved_scenario_payload(ATTRIBUTES, PRO)

    assert "version" not in body["saved_scenario"]


def test_an_explicit_version_is_not_overridden():
    attributes = {**ATTRIBUTES, "version": "beta"}

    body = saved_scenario_payload(attributes, STABLE)

    assert body["saved_scenario"]["version"] == "beta"
