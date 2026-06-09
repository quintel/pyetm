"""Error handling policy for pyetm models.

This module defines how warnings and errors are handled based on:
1. Error mode (safe/default/dangerous) from PYETM_ERROR_MODE env var
2. Warning severity (error/warning/info)
3. Execution context (single operation vs bulk operation)

Error Modes:
    - safe: All warnings raise exceptions (maximum safety for CI/production)
    - default: Only severity="error" warnings raise in single operations (smart behavior)
    - dangerous: No warnings raise, all are logged (useful for exploratory analysis)

Examples:
    >>> # Single operation, default mode
    >>> scenario.update(inputs={"bad_key": 123})  # Raises if validation fails

    >>> # Bulk operation, default mode
    >>> scenarios.create_many([...])  # Collects warnings, continues processing

    >>> # Safe mode (PYETM_ERROR_MODE=safe)
    >>> scenarios.create_many([...])  # Raises on first error
"""

from enum import Enum
from functools import lru_cache
from typing import Literal


class ErrorMode(str, Enum):
    """Error handling mode for pyetm operations."""

    SAFE = "safe"  # All warnings raise exceptions
    DEFAULT = "default"  # Smart behavior: raise on error severity in single ops
    DANGEROUS = "dangerous"  # Nothing raises, all warnings logged


class ErrorPolicy:
    """Determines whether warnings should raise exceptions based on context and mode."""

    def __init__(self, mode: ErrorMode = ErrorMode.DEFAULT) -> None:
        """Initialize error policy with specified mode.

        Args:
            mode: Error handling mode (safe/default/dangerous)
        """
        self.mode = mode

    def should_raise(
        self,
        severity: Literal["info", "warning", "error"],
        is_bulk_context: bool = False,
    ) -> bool:
        """Determine if a warning should raise an exception.

        Args:
            severity: Warning severity level
            is_bulk_context: True if warning occurred during bulk operation

        Returns:
            True if warning should raise an exception, False to log only

        Decision Matrix:
            SAFE mode:      Always raise (all severities, all contexts)
            DANGEROUS mode: Never raise (all severities, all contexts)
            DEFAULT mode:
                - Single operations: Raise on severity="error"
                - Bulk operations:   Never raise (collect warnings)
        """
        if self.mode == ErrorMode.SAFE:
            return True

        if self.mode == ErrorMode.DANGEROUS:
            return False

        # DEFAULT mode: context-aware behavior
        if is_bulk_context:
            # In bulk operations, collect warnings instead of raising
            return False

        # In single operations, raise only on error severity
        return severity == "error"


@lru_cache(maxsize=1)
def get_error_policy() -> ErrorPolicy:
    """Get singleton ErrorPolicy instance based on current settings.

    Returns:
        ErrorPolicy configured according to PYETM_ERROR_MODE env var

    Note:
        This function is cached to avoid repeated settings imports.
        Clear cache with get_error_policy.cache_clear() if settings change.
    """
    from pyetm.config.settings import get_settings

    settings = get_settings()
    mode = ErrorMode(settings.error_mode)
    return ErrorPolicy(mode=mode)
