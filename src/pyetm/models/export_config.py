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
    inputs_defaults: Optional[bool] = None
    inputs_min_max: Optional[bool] = None

    # Select which output carriers to include; None means don't include carriers
    output_carriers: Optional[Sequence[str]] = None

    def effective_bool(self, value: Optional[bool], default: bool) -> bool:
        return default if value is None else bool(value)
