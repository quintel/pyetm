"""PyETM - Python client for the Energy Transition Model API."""

from importlib.metadata import version

# Core models
from pyetm.models import (
    Scenario,
    Scenarios,
    Session,
    Sessions,
    ScenarioPacker,
    Collection,
)

# Sub-models
from pyetm.models import (
    Inputs,
    CustomCurves,
    Gqueries,
)

# Client
from pyetm.clients import BaseClient, get_client

__version__ = version("pyetm")

__all__ = [
    "__version__",
    # Core models
    "Scenario",
    "Scenarios",
    "Session",
    "Sessions",
    "ScenarioPacker",
    "Collection"
    # Sub-models
    "Inputs",
    "CustomCurves",
    "Gqueries",
    # Client
    "BaseClient",
    "get_client",
]
