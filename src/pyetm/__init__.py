"""PyETM - Python client for the Energy Transition Model API."""

from importlib.metadata import version
from typing import Any

from pyetm.clients import BaseClient, get_client
from pyetm.models import (
    Collection,
    CustomCurves,
    Gqueries,
    Inputs,
    Scenario,
    ScenarioPacker,
    Scenarios,
)

__version__ = version("pyetm")

__all__ = [
    "BaseClient",
    "Collection",
    "CustomCurves",
    "Gqueries",
    "Inputs",
    "Scenario",
    "ScenarioPacker",
    "Scenarios",
    "__version__",
    "get_client",
]

# TEMPORARY(2.2): names removed in 2.1, which say what replaced them for one release
_REMOVED = {"Session": "Scenario", "Sessions": "Scenarios"}


def __getattr__(name: str) -> Any:
    if name in _REMOVED:
        raise ImportError(
            f"pyetm.{name} was removed in 2.1. Use {_REMOVED[name]} instead: every scenario is a "
            f"saved scenario now. See the 2.1.0 changelog for each replacement."
        )
    raise AttributeError(f"module 'pyetm' has no attribute {name!r}")
