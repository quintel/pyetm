"""PyETM - Python client for the Energy Transition Model API."""

from importlib.metadata import version

# Core models
from pyetm.models import (
    Scenario,
    Scenarios,
    Session,
    Sessions,
    ScenarioPacker,
)

# Sub-models
from pyetm.models import (
    Inputs,
    CustomCurves,
    Gqueries,
)

# Client
from pyetm.clients import BaseClient

__version__ = version("pyetm")

__all__ = [
    "__version__",
    # Core models
    "Scenario",
    "Scenarios",
    "Session",
    "Sessions",
    "ScenarioPacker",
    # Sub-models
    "Inputs",
    "CustomCurves",
    "Gqueries",
    # Client
    "BaseClient",
]
