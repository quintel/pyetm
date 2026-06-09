"""Tests for error policy and error handling modes."""

import pytest
from unittest.mock import patch, MagicMock
from pyetm.models.error_policy import ErrorMode, ErrorPolicy, get_error_policy


class TestErrorMode:
    """Test ErrorMode enum values."""

    def test_error_mode_values(self):
        """Test that ErrorMode has expected values."""
        assert ErrorMode.SAFE == "safe"
        assert ErrorMode.DEFAULT == "default"
        assert ErrorMode.DANGEROUS == "dangerous"

    def test_error_mode_from_string(self):
        """Test ErrorMode can be created from string."""
        assert ErrorMode("safe") == ErrorMode.SAFE
        assert ErrorMode("default") == ErrorMode.DEFAULT
        assert ErrorMode("dangerous") == ErrorMode.DANGEROUS


class TestErrorPolicy:
    """Test ErrorPolicy decision logic."""

    def test_safe_mode_always_raises(self):
        """In safe mode, all warnings should raise regardless of severity or context."""
        policy = ErrorPolicy(mode=ErrorMode.SAFE)

        # All severities should raise
        assert policy.should_raise("info", is_bulk_context=False) is True
        assert policy.should_raise("warning", is_bulk_context=False) is True
        assert policy.should_raise("error", is_bulk_context=False) is True

        # Even in bulk context
        assert policy.should_raise("info", is_bulk_context=True) is True
        assert policy.should_raise("warning", is_bulk_context=True) is True
        assert policy.should_raise("error", is_bulk_context=True) is True

    def test_dangerous_mode_never_raises(self):
        """In dangerous mode, no warnings should raise."""
        policy = ErrorPolicy(mode=ErrorMode.DANGEROUS)

        # No severities should raise
        assert policy.should_raise("info", is_bulk_context=False) is False
        assert policy.should_raise("warning", is_bulk_context=False) is False
        assert policy.should_raise("error", is_bulk_context=False) is False

        # Even in single operation context
        assert policy.should_raise("info", is_bulk_context=True) is False
        assert policy.should_raise("warning", is_bulk_context=True) is False
        assert policy.should_raise("error", is_bulk_context=True) is False

    def test_default_mode_single_operation(self):
        """In default mode, only error severity raises in single operations."""
        policy = ErrorPolicy(mode=ErrorMode.DEFAULT)

        # Single operation context - only error severity raises
        assert policy.should_raise("info", is_bulk_context=False) is False
        assert policy.should_raise("warning", is_bulk_context=False) is False
        assert policy.should_raise("error", is_bulk_context=False) is True

    def test_default_mode_bulk_operation(self):
        """In default mode, nothing raises in bulk operations."""
        policy = ErrorPolicy(mode=ErrorMode.DEFAULT)

        # Bulk operation context - nothing raises
        assert policy.should_raise("info", is_bulk_context=True) is False
        assert policy.should_raise("warning", is_bulk_context=True) is False
        assert policy.should_raise("error", is_bulk_context=True) is False


class TestGetErrorPolicy:
    """Test get_error_policy singleton function."""

    def test_get_error_policy_returns_instance(self):
        """Test that get_error_policy returns an ErrorPolicy instance."""
        # Clear cache before test
        get_error_policy.cache_clear()

        with patch("pyetm.config.settings.get_settings") as mock_get_settings:
            mock_settings = MagicMock()
            mock_settings.error_mode = "default"
            mock_get_settings.return_value = mock_settings

            policy = get_error_policy()
            assert isinstance(policy, ErrorPolicy)
            assert policy.mode == ErrorMode.DEFAULT

    def test_get_error_policy_respects_safe_mode(self):
        """Test that get_error_policy respects safe mode from settings."""
        get_error_policy.cache_clear()

        with patch("pyetm.config.settings.get_settings") as mock_get_settings:
            mock_settings = MagicMock()
            mock_settings.error_mode = "safe"
            mock_get_settings.return_value = mock_settings

            policy = get_error_policy()
            assert policy.mode == ErrorMode.SAFE
            assert policy.should_raise("warning", is_bulk_context=True) is True

    def test_get_error_policy_respects_dangerous_mode(self):
        """Test that get_error_policy respects dangerous mode from settings."""
        get_error_policy.cache_clear()

        with patch("pyetm.config.settings.get_settings") as mock_get_settings:
            mock_settings = MagicMock()
            mock_settings.error_mode = "dangerous"
            mock_get_settings.return_value = mock_settings

            policy = get_error_policy()
            assert policy.mode == ErrorMode.DANGEROUS
            assert policy.should_raise("error", is_bulk_context=False) is False

    def test_get_error_policy_is_cached(self):
        """Test that get_error_policy caches the result."""
        get_error_policy.cache_clear()

        with patch("pyetm.config.settings.get_settings") as mock_get_settings:
            mock_settings = MagicMock()
            mock_settings.error_mode = "default"
            mock_get_settings.return_value = mock_settings

            # First call should create instance
            policy1 = get_error_policy()
            # Second call should return cached instance
            policy2 = get_error_policy()

            # Should be the same instance
            assert policy1 is policy2
            # get_settings should only be called once due to caching
            assert mock_get_settings.call_count == 1


class TestErrorPolicyDecisionMatrix:
    """Test complete decision matrix for all mode/severity/context combinations."""

    @pytest.mark.parametrize(
        "mode,severity,is_bulk,expected",
        [
            # SAFE mode - always raise
            (ErrorMode.SAFE, "info", False, True),
            (ErrorMode.SAFE, "info", True, True),
            (ErrorMode.SAFE, "warning", False, True),
            (ErrorMode.SAFE, "warning", True, True),
            (ErrorMode.SAFE, "error", False, True),
            (ErrorMode.SAFE, "error", True, True),
            # DEFAULT mode - error raises in single ops only
            (ErrorMode.DEFAULT, "info", False, False),
            (ErrorMode.DEFAULT, "info", True, False),
            (ErrorMode.DEFAULT, "warning", False, False),
            (ErrorMode.DEFAULT, "warning", True, False),
            (ErrorMode.DEFAULT, "error", False, True),
            (ErrorMode.DEFAULT, "error", True, False),
            # DANGEROUS mode - never raise
            (ErrorMode.DANGEROUS, "info", False, False),
            (ErrorMode.DANGEROUS, "info", True, False),
            (ErrorMode.DANGEROUS, "warning", False, False),
            (ErrorMode.DANGEROUS, "warning", True, False),
            (ErrorMode.DANGEROUS, "error", False, False),
            (ErrorMode.DANGEROUS, "error", True, False),
        ],
    )
    def test_decision_matrix(self, mode, severity, is_bulk, expected):
        """Test complete decision matrix for all combinations."""
        policy = ErrorPolicy(mode=mode)
        result = policy.should_raise(severity, is_bulk_context=is_bulk)
        assert result == expected, (
            f"Expected should_raise({severity}, is_bulk_context={is_bulk}) "
            f"to be {expected} in {mode} mode, but got {result}"
        )
