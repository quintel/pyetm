"""Export configuration models."""

from __future__ import annotations

from typing import Optional, Sequence
from pydantic import BaseModel


class ExportConfig(BaseModel):
    """
    Per-scenario export configuration.

    If a value is None, the exporter will use its default/global behavior.
    """

    include_inputs: Optional[bool] = None
    include_sortables: Optional[bool] = None
    include_custom_curves: Optional[bool] = None
    include_gqueries: Optional[bool] = None
    include_users: Optional[bool] = None
    include_input_defaults: Optional[bool] = None
    include_input_min_max: Optional[bool] = None

    # Select which hourly curves to export; None or empty list means don't include hourly curves
    # Can be a list of carrier names (e.g., ["electricity", "heat"]) or True to export all
    hourly_curves: Optional[Sequence[str]] = None

    # Select which annual exports to include; None means don't include any annual exports
    include_annual_exports: Optional[Sequence[str]] = None

    def effective_bool(self, value: Optional[bool], default: bool) -> bool:
        return default if value is None else bool(value)
