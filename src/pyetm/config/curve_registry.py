"""Single source of truth for hourly output curve names.

Each logical curve has a canonical key, used inside pyetm (cache files, ``curve.key``) and in
the request URL ``/scenarios/{id}/curves/{canonical}.csv``. etengine renamed four curves
(``merit_order`` -> ``electricity_profiles``, ``heat_network`` -> ``district_heating_profiles``,
``hydrogen`` -> ``hydrogen_profiles``, ``network_gas`` -> ``network_gas_profiles``); callers may
still pass the old names, which resolve to the canonical key with a deprecation warning.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Optional

logger = logging.getLogger("pyetm")


@dataclass(frozen=True)
class CurveSpec:
    """Describes one logical curve."""

    canonical: str
    curve_type: str
    carrier: Optional[str] = None


_SPECS: tuple[CurveSpec, ...] = (
    CurveSpec("electricity_profiles", "merit_curve", carrier="electricity"),
    CurveSpec("district_heating_profiles", "load_curve", carrier="heat"),
    CurveSpec("hydrogen_profiles", "reconciliation_curve", carrier="hydrogen"),
    CurveSpec("network_gas_profiles", "reconciliation_curve", carrier="methane"),
    CurveSpec("electricity_price", "price_curve"),
    CurveSpec("agriculture_heat", "merit_curve"),
    CurveSpec("household_heat", "fever_curve"),
    CurveSpec("buildings_heat", "fever_curve"),
    CurveSpec("residual_load", "query_curve"),
    CurveSpec("hydrogen_integral_cost", "query_curve"),
)

_BY_CANONICAL: dict[str, CurveSpec] = {s.canonical: s for s in _SPECS}

# Old names accepted from callers, mapped to their canonical key. These trigger a deprecation
# warning via accept_alias().
_DEPRECATED_ALIASES: dict[str, str] = {
    "merit_order": "electricity_profiles",
    "heat_network": "district_heating_profiles",
    "hydrogen": "hydrogen_profiles",
    "network_gas": "network_gas_profiles",
}


def canonical_names() -> list[str]:
    """Canonical key for every registered curve."""
    return [s.canonical for s in _SPECS]


def accept_alias(name: str) -> str:
    """Resolve a legacy/old curve name to its canonical key.

    Emits a deprecation warning for old engine names; unknown names pass through unchanged so
    non-registered identifiers still reach the engine.
    """
    if name in _BY_CANONICAL:
        return name
    canonical = _DEPRECATED_ALIASES.get(name)
    if canonical is None:
        return name
    logger.warning("Curve name '%s' is deprecated; use '%s' instead.", name, canonical)
    return canonical


def get_spec(name: str) -> Optional[CurveSpec]:
    """Spec for a curve by canonical or alias name, or None if not registered."""
    return _BY_CANONICAL.get(accept_alias(name))


def curve_type_for(name: str) -> str:
    """pyetm curve type tag for a curve name."""
    spec = get_spec(name)
    return spec.curve_type if spec else "output_curve"


def carrier_to_canonical() -> dict[str, str]:
    """Carrier alias -> canonical curve name (primary curve per carrier)."""
    return {s.carrier: s.canonical for s in _SPECS if s.carrier}


def build_path(session_id: Any, name: str) -> str:
    """Request path for an output curve: /scenarios/{id}/curves/{canonical}.csv."""
    return f"/scenarios/{session_id}/curves/{accept_alias(name)}.csv"
