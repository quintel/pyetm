"""Validation utilities for pyetm curve and export types.

This module provides validation functions that raise clear ValueError exceptions
when invalid curve types, carrier types, or export names are provided.
"""

from __future__ import annotations

from typing import TypeVar, get_args, Any, cast
from pydantic import TypeAdapter, ValidationError
from pyetm.types import CarrierType
from pyetm.config import curve_registry
from pyetm.services.curve_metadata_service import CurveMetadataService

T = TypeVar("T")


def _validate_literal_type(
    value: str | list[str],
    literal_type: Any,  # Accept Literal type aliases
    error_label: str,
    singular: bool = False,
) -> Any:
    """Generic helper to validate values against a Literal type."""
    if singular:
        values_to_validate = value
    else:
        values_to_validate = [value] if isinstance(value, str) else value

    adapter = TypeAdapter(literal_type if singular else list[literal_type])
    try:
        return adapter.validate_python(values_to_validate)
    except ValidationError as e:
        if singular:
            invalid_display = f"'{value}'"
        else:
            invalid = [
                error["input"]
                for error in e.errors()
                if error["type"] == "literal_error" and "input" in error
            ]
            invalid_display = str(invalid)

        # Build user-friendly error message
        valid_types = ", ".join(get_args(literal_type))
        colon = ":" if not singular else ""
        raise ValueError(
            f"Invalid {error_label}{colon} {invalid_display}. Valid types: {valid_types}"
        ) from None


def _validate_against_list(
    value: str | list[str],
    valid_values: list[str],
    error_label: str,
) -> list[str]:
    """Generic helper to validate values against a list of valid values."""
    values_to_validate = [value] if isinstance(value, str) else value

    invalid = [v for v in values_to_validate if v not in valid_values]
    if invalid:
        valid_types = ", ".join(valid_values)
        raise ValueError(
            f"Invalid {error_label}: {invalid}. Valid types: {valid_types}"
        )

    return values_to_validate


def validate_carrier_type(carrier_type: str) -> str:
    """Validate that carrier_type is a valid carrier type."""
    return cast(
        str,
        _validate_literal_type(carrier_type, CarrierType, "carrier type", singular=True),
    )


def validate_export_names(export_names: str | list[str]) -> list[str]:
    """
    Validate and normalize export names.

    Fetches valid export names from ETEngine metadata API.

    Note: may trigger a cached metadata fetch from ETEngine on first use
    (falls back to curve_registry / built-in defaults offline).
    """
    valid_exports = CurveMetadataService.get_export_names()
    return _validate_against_list(export_names, valid_exports, "export names")


def validate_hourly_curve_names(curve_names: str | list[str]) -> list[str]:
    """
    Validate and normalize hourly curve names.

    Legacy/old engine names (e.g. ``merit_order``) are resolved to their canonical
    key first, then validated against the curve names from the ETEngine metadata API.

    Note: may trigger a cached metadata fetch from ETEngine on first use
    (falls back to curve_registry / built-in defaults offline).
    """
    names = [curve_names] if isinstance(curve_names, str) else list(curve_names)
    normalized = [curve_registry.accept_alias(name) for name in names]
    valid_curves = CurveMetadataService.get_curve_names()
    return _validate_against_list(normalized, valid_curves, "curve names")
