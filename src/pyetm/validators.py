"""Validation utilities for pyetm curve and export types.

This module provides validation functionsthat raise clear ValueError exceptions
when invalid curve types, carrier types, or export names are provided.
"""

from typing import TypeVar, get_args, Any, cast
from pydantic import TypeAdapter, ValidationError
from pyetm.types import AnnualExportType, CarrierType, HourlyCurveType

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


def validate_carrier_type(carrier_type: str) -> str:
    """Validate that carrier_type is a valid carrier type."""
    return cast(
        str,
        _validate_literal_type(carrier_type, CarrierType, "carrier type", singular=True),
    )


def validate_export_names(export_names: str | list[str]) -> list[str]:
    """Validate and normalize export names."""
    return cast(
        list[str], _validate_literal_type(export_names, AnnualExportType, "export names")
    )


def validate_hourly_curve_names(curve_names: str | list[str]) -> list[str]:
    """Validate and normalize hourly curve names."""
    return cast(
        list[str], _validate_literal_type(curve_names, HourlyCurveType, "curve names")
    )
