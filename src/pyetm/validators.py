"""Validation utilities for pyetm curve and export types.

This module provides validation functions that raise clear ValueError exceptions
when invalid curve types, carrier types, or export names are provided.
"""

from pyetm.types import (
    VALID_ANNUAL_EXPORT_TYPES,
    VALID_CARRIER_TYPES,
    VALID_HOURLY_CURVE_TYPES,
)


def validate_carrier_type(carrier_type: str) -> str:
    """Validate that carrier_type is a valid carrier type."""
    if carrier_type not in VALID_CARRIER_TYPES:
        valid_types = ", ".join(VALID_CARRIER_TYPES)
        raise ValueError(
            f"Invalid carrier type '{carrier_type}'. Valid types: {valid_types}"
        )
    return carrier_type


def validate_export_names(export_names: str | list[str]) -> list[str]:
    """Validate and normalize export names.

    Auto-converts a single string to a list. Validates that all export names
    are valid annual export types.
    """
    if isinstance(export_names, str):
        export_names = [export_names]

    # Validate all export names
    invalid = [name for name in export_names if name not in VALID_ANNUAL_EXPORT_TYPES]
    if invalid:
        valid_types = ", ".join(VALID_ANNUAL_EXPORT_TYPES)
        raise ValueError(f"Invalid export names: {invalid}. Valid types: {valid_types}")

    return export_names


def validate_hourly_curve_names(curve_names: str | list[str]) -> list[str]:
    """Validate and normalize hourly curve names.

    Auto-converts a single string to a list. Validates that all curve names
    are valid hourly curve types.
    """
    if isinstance(curve_names, str):
        curve_names = [curve_names]

    # Validate all curve names
    invalid = [name for name in curve_names if name not in VALID_HOURLY_CURVE_TYPES]
    if invalid:
        valid_types = ", ".join(VALID_HOURLY_CURVE_TYPES)
        raise ValueError(f"Invalid curve names: {invalid}. Valid types: {valid_types}")

    return curve_names
