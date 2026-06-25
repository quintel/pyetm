"""Single source of truth for hourly output curve names.

This registry covers only the **hourly** output cluster — 8760-row curves served from
``/scenarios/{id}/curves/{wire}.csv``. Capacities are a table shape and belong to the
**annual** export cluster (``/scenarios/{id}/{name}``); they are handled by the
AnnualExports model and discovered via ``/exports/metadata``, not here.

etengine renamed the hourly curve endpoints (``merit_order`` -> ``electricity_profiles``,
``heat_network`` -> ``district_heating_profiles``, ``hydrogen`` -> ``hydrogen_profiles``,
``network_gas`` -> ``network_gas_profiles``) but kept the OLD names as backwards-compatible
aliases. The old names therefore work on BOTH modern engines (pro) and stable engines
(2025-01, which only ever had the old names), so pyetm sends them as the on-the-wire name
and needs no per-engine logic.

Each logical curve maps to:
- ``canonical``: the stable, public key used inside pyetm (cache files, ``curve.key``); the
  modern name.
- ``wire``: the name sent in the request URL (the universal old name where one exists).

This module is the offline fallback for :class:`CurveMetadataService`; the live
``/curves/metadata`` endpoint is always preferred when reachable.
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
    wire: str
    curve_type: str
    carrier: Optional[str] = None


_SPECS: tuple[CurveSpec, ...] = (
    # Renamed hourly profile curves: send the universal old name (works on pro + 2025-01)
    CurveSpec("electricity_profiles", "merit_order", "merit_curve", carrier="electricity"),
    CurveSpec("district_heating_profiles", "heat_network", "load_curve", carrier="heat"),
    CurveSpec("hydrogen_profiles", "hydrogen", "reconciliation_curve", carrier="hydrogen"),
    CurveSpec("network_gas_profiles", "network_gas", "reconciliation_curve", carrier="methane"),
    # Unchanged curves (same name on both engines)
    CurveSpec("electricity_price", "electricity_price", "price_curve"),
    CurveSpec("agriculture_heat", "agriculture_heat", "merit_curve"),
    CurveSpec("household_heat", "household_heat", "fever_curve"),
    CurveSpec("buildings_heat", "buildings_heat", "fever_curve"),
    CurveSpec("residual_load", "residual_load", "query_curve"),
    CurveSpec("hydrogen_integral_cost", "hydrogen_integral_cost", "query_curve"),
)
# Capacities are annual exports, not hourly curves (table shape, served from the
# /exports endpoint). They live in the annual-export cluster — see
# CurveMetadataService._DEFAULT_EXPORTS and the AnnualExports model.

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


def default_curve_metadata() -> list[dict[str, str]]:
    """Offline fallback metadata for every registered curve.

    Mirrors the shape of ETEngine's ``/curves/metadata`` response so
    :class:`CurveMetadataService` can fall back to it when the endpoint is unavailable.
    """
    return [{"name": s.canonical, "type": s.curve_type, "description": ""} for s in _SPECS]


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
    """Request path for an output curve or capacity on the unified curves route.

    ``/scenarios/{id}/curves/{wire_name}.csv`` — renamed curves send their universal old
    name; everything else sends its own name.
    """
    spec = get_spec(name)
    wire = spec.wire if spec else name
    return f"/scenarios/{session_id}/curves/{wire}.csv"
