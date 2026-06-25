"""Single source of truth for hourly output curve / capacity export names.

etengine renamed the hourly curve endpoints (``merit_order`` -> ``electricity_profiles``,
``heat_network`` -> ``district_heating_profiles``, ``hydrogen`` -> ``hydrogen_profiles``,
``network_gas`` -> ``network_gas_profiles``) but kept the OLD names as backwards-compatible
aliases. The old names therefore work on BOTH modern engines (pro) and stable engines
(2025-01, which only ever had the old names), so pyetm uses them as the on-the-wire name and
needs no per-engine logic.

Each logical curve maps to:
- ``canonical``: the stable, public key used inside pyetm (cache files, ``curve.key``); the
  modern name.
- ``wire``: the name sent in the request URL (the universal old name where one exists).
- ``endpoint``: ``curves`` -> /scenarios/{id}/curves/{name}.csv;
  ``export`` -> /scenarios/{id}/{name} (scenario-member, like annual exports). Capacity exports
  are modern-only and simply 404 (and are skipped) on stable engines.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Literal, Optional

logger = logging.getLogger("pyetm")

Endpoint = Literal["curves", "export"]


@dataclass(frozen=True)
class CurveSpec:
    """Describes one logical curve."""

    canonical: str
    wire: str
    endpoint: Endpoint
    curve_type: str
    carrier: Optional[str] = None


_SPECS: tuple[CurveSpec, ...] = (
    # Renamed hourly profile curves: send the universal old name (works on pro + 2025-01)
    CurveSpec("electricity_profiles", "merit_order", "curves", "merit_curve", carrier="electricity"),
    CurveSpec("district_heating_profiles", "heat_network", "curves", "load_curve", carrier="heat"),
    CurveSpec("hydrogen_profiles", "hydrogen", "curves", "reconciliation_curve", carrier="hydrogen"),
    CurveSpec("network_gas_profiles", "network_gas", "curves", "reconciliation_curve", carrier="methane"),
    # Unchanged curves (same name on both engines)
    CurveSpec("electricity_price", "electricity_price", "curves", "price_curve"),
    CurveSpec("agriculture_heat", "agriculture_heat", "curves", "merit_curve"),
    CurveSpec("household_heat", "household_heat", "curves", "fever_curve"),
    CurveSpec("buildings_heat", "buildings_heat", "curves", "fever_curve"),
    CurveSpec("residual_load", "residual_load", "curves", "query_curve"),
    CurveSpec("hydrogen_integral_cost", "hydrogen_integral_cost", "curves", "query_curve"),
    # Capacity exports (modern-only, scenario-member export routes; 404 -> skipped on 2025-01)
    CurveSpec("electricity_capacities", "electricity_capacities", "export", "capacity_curve"),
    CurveSpec("district_heating_capacities", "district_heating_capacities", "export", "capacity_curve"),
    CurveSpec("hydrogen_capacities", "hydrogen_capacities", "export", "capacity_curve"),
    CurveSpec("network_gas_capacities", "network_gas_capacities", "export", "capacity_curve"),
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
    """Request path for an output curve / capacity export.

    ``curves``  -> /scenarios/{id}/curves/{wire_name}.csv
    ``export``  -> /scenarios/{id}/{name}   (scenario-member, mirrors fetch_annual_exports)
    """
    spec = get_spec(name)
    if spec and spec.endpoint == "export":
        return f"/scenarios/{session_id}/{spec.canonical}"
    wire = spec.wire if spec else name
    return f"/scenarios/{session_id}/curves/{wire}.csv"
