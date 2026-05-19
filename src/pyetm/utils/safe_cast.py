"""
Methods that help with type conversion from eg excel
"""

from typing import Any, Optional

import pandas as pd


def cast_int(value: Any) -> Optional[int]:
    """Safely convert value to integer."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return None


def cast_bool(value: Any) -> Optional[bool]:
    """Safely convert value to boolean."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        try:
            return bool(int(value))
        except Exception:
            return None
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "yes", "y", "1"}:
            return True
        if normalized in {"false", "no", "n", "0"}:
            return False
    return None
