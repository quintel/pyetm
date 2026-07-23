"""Type definitions for pyetm curve and export types."""

from typing import Literal

# Carrier types for hourly output curves (remains static)
CarrierType = Literal["electricity", "heat", "hydrogen", "methane"]

# Export and curve names are server-driven: the set of valid names lives in
# ETEngine (see services.curve_metadata_service) and can change without a pyetm
# release. They are therefore plain ``str`` aliases rather than ``Literal`` — we
# trade static autocomplete for not having to ship a new pyetm version whenever a
# curve is added. The names are validated at runtime instead; the aliases are
# kept for readable signatures and to mark intent.
# Validate with validators.validate_export_names() / validate_hourly_curve_names().
AnnualExportType = str
HourlyCurveType = str
