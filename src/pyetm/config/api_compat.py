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


def _environment_label(base_url: str | None) -> str | None:
    """Environment label used to build this host, or None for production/local."""
    if not base_url:
        return None

    host = urlparse(base_url).hostname or ""
    label = host.split(".")[0]

    if label in ("", "engine", "localhost"):
        return None

    return label


def saved_scenario_payload(
    attributes: dict[str, Any], base_url: str | None = None
) -> dict[str, Any]:
    """Build a saved scenario body the target engine can read.

    2025-01 reads attributes from the top level; later engines from a
    ``saved_scenario`` root key. Also stamps ``version`` with the environment
    label, since MyETM defaults an unlabelled scenario to production.
    """
    body = dict(attributes)

    label = _environment_label(base_url)
    if label is not None:
        body.setdefault("version", label)

    if _targets_stable_2025_01(base_url):
        return body

    return {"saved_scenario": body}
