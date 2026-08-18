"""The saved scenario body differs between the stable engine and everything newer.

Each reader below mirrors how one engine version pulls attributes out of the request,
so a payload change that would break either deployment fails here.
"""

from typing import Any

import pytest

from pyetm.config.api_compat import saved_scenario_payload

ATTRIBUTES = {"scenario_id": 123, "title": "Fix check", "private": False}
PERMITTED = ("scenario_id", "title", "description", "private")

STABLE = "https://2025-01.engine.energytransitionmodel.com/api/v3"
PRO = "https://engine.energytransitionmodel.com/api/v3"


def read_as_2025_01(body: dict[str, Any]) -> dict[str, Any]:
    """CreateSavedScenario::Contract at tag stable.2025.01 reads top-level keys."""
    return {key: body[key] for key in PERMITTED if key in body}


def read_as_2026_01(body: dict[str, Any]) -> dict[str, Any]:
    """params.require(:saved_scenario) at tag 2026-01 and later reads the root key.

    The KeyError stands in for ActionController::ParameterMissing.
    """
    nested = body["saved_scenario"]
    return {key: nested[key] for key in PERMITTED if key in nested}


def test_stable_engine_receives_the_attributes_at_the_top_level():
    body = saved_scenario_payload(ATTRIBUTES, STABLE)

    assert read_as_2025_01(body) == ATTRIBUTES


@pytest.mark.parametrize(
    "base_url",
    [
        PRO,
        "https://beta.engine.energytransitionmodel.com/api/v3",
        "http://localhost:3000/api/v3",
        "https://2025-01-collections.energytransitionmodel.com/api/v3",
        None,
    ],
)
def test_every_other_engine_receives_the_root_key(base_url):
    body = saved_scenario_payload(ATTRIBUTES, base_url)

    assert read_as_2026_01(body) == ATTRIBUTES


def test_the_stable_body_carries_no_root_key():
    """A root key would stop the 2025-01 contract seeing the attributes at all."""
    assert "saved_scenario" not in saved_scenario_payload(ATTRIBUTES, STABLE)
