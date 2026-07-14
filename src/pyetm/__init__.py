"""PyETM - Python client for the Energy Transition Model API."""

from importlib.metadata import version

# Client
from pyetm.clients import BaseClient, get_client

# Core models
# Sub-models
from pyetm.models import (
    Collection,
    CustomCurves,
    Gqueries,
    Inputs,
    Scenario,
    ScenarioPacker,
    Scenarios,
    Session,
    Sessions,
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
    "Session",
    "Sessions",
    "__version__",
    "get_client",
]
