"""Request shapes that work across every engine version pyetm targets.

Sibling of :mod:`pyetm.config.curve_registry`, which does the same for curve names.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import urlparse

STABLE_2025_01_LABEL = "2025-01"


def _targets_stable_2025_01(base_url: str | None) -> bool:
    """Is this client pointed at the 2025-01 stable engine?"""
    if not base_url:
        return False

    host = urlparse(base_url).hostname or ""
    return host.split(".")[0] == STABLE_2025_01_LABEL


def saved_scenario_payload(
    attributes: dict[str, Any], base_url: str | None = None
) -> dict[str, Any]:
    """Build a saved scenario body the target engine can read.

    Stable 2025-01 reads the attributes from the top level of the request; every
    engine from 2026-01 onwards reads them from a ``saved_scenario`` root key. The
    root key doubles as the signal that keeps Rails' ParamsWrapper from choosing the
    shape for us, so it is what anything other than 2025-01 gets.
    """
    if _targets_stable_2025_01(base_url):
        return dict(attributes)

    return {"saved_scenario": attributes}
