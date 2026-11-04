"""Saved scenario request bodies, labelled with the environment they belong to."""

from __future__ import annotations

from typing import Any
from urllib.parse import urlparse


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
    """Build a saved scenario body under the ``saved_scenario`` root key.

    Stamps ``version`` with the environment label, since MyETM defaults an
    unlabelled scenario to production.
    """
    body = dict(attributes)

    label = _environment_label(base_url)
    if label is not None:
        body.setdefault("version", label)

    return {"saved_scenario": body}
