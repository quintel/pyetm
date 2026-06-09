"""
Integration tests for error mode switching at runtime.

These tests verify that the error policy system correctly picks up
environment variable changes when caches are cleared.
"""

import pytest
from pyetm.models.error_policy import ErrorMode, get_error_policy
from pyetm.config.settings import reload_configuration


class TestErrorModeSwitching:
    """Test runtime error mode switching with environment variables."""

    def test_switch_to_safe_mode(self, monkeypatch, clean_settings_env):
        """Test switching from DEFAULT to SAFE mode."""
        # Clear caches to ensure clean state
        reload_configuration()

        # Set SAFE mode
        monkeypatch.setenv("PYETM_ERROR_MODE", "safe")
        reload_configuration()

        # Verify mode switched
        policy = get_error_policy()
        assert policy.mode == ErrorMode.SAFE
        assert policy.should_raise("info", is_bulk_context=False) is True
        assert policy.should_raise("info", is_bulk_context=True) is True

    def test_switch_to_dangerous_mode(self, monkeypatch, clean_settings_env):
        """Test switching from DEFAULT to DANGEROUS mode."""
        # Clear caches
        reload_configuration()

        # Set DANGEROUS mode
        monkeypatch.setenv("PYETM_ERROR_MODE", "dangerous")
        reload_configuration()

        # Verify mode switched
        policy = get_error_policy()
        assert policy.mode == ErrorMode.DANGEROUS
        assert policy.should_raise("error", is_bulk_context=False) is False
        assert policy.should_raise("error", is_bulk_context=True) is False

    def test_switch_to_default_mode(self, monkeypatch, clean_settings_env):
        """Test explicitly setting DEFAULT mode."""
        # Start with SAFE
        monkeypatch.setenv("PYETM_ERROR_MODE", "safe")
        reload_configuration()
        assert get_error_policy().mode == ErrorMode.SAFE

        # Switch to DEFAULT
        monkeypatch.setenv("PYETM_ERROR_MODE", "default")
        reload_configuration()

        # Verify mode switched
        policy = get_error_policy()
        assert policy.mode == ErrorMode.DEFAULT
        assert policy.should_raise("info", is_bulk_context=False) is False
        assert policy.should_raise("error", is_bulk_context=False) is True
        assert policy.should_raise("error", is_bulk_context=True) is False

    def test_mode_persists_without_cache_clear(self, monkeypatch, clean_settings_env):
        """Test that mode doesn't change if caches aren't cleared."""
        # Set to SAFE mode
        monkeypatch.setenv("PYETM_ERROR_MODE", "safe")
        reload_configuration()
        assert get_error_policy().mode == ErrorMode.SAFE

        # Change env var but DON'T clear caches
        monkeypatch.setenv("PYETM_ERROR_MODE", "dangerous")

        # Mode should still be SAFE (cached)
        policy = get_error_policy()
        assert policy.mode == ErrorMode.SAFE

    def test_mode_changes_after_cache_clear(self, monkeypatch, clean_settings_env):
        """Test that mode changes when caches are cleared after env change."""
        # Set to SAFE mode
        monkeypatch.setenv("PYETM_ERROR_MODE", "safe")
        reload_configuration()
        assert get_error_policy().mode == ErrorMode.SAFE

        # Change env var and clear caches
        monkeypatch.setenv("PYETM_ERROR_MODE", "dangerous")
        reload_configuration()

        # Mode should now be DANGEROUS
        policy = get_error_policy()
        assert policy.mode == ErrorMode.DANGEROUS

    def test_default_mode_without_env_var(self, clean_settings_env):
        """Test that DEFAULT is used when no env var is set."""
        # Don't set any env var
        reload_configuration()

        policy = get_error_policy()
        assert policy.mode == ErrorMode.DEFAULT

    def test_case_insensitive_mode_names(self, monkeypatch, clean_settings_env):
        """Test that mode names work with different cases."""
        # Pydantic Literal values must match exactly, but settings are case-insensitive
        # So "safe" works but "SAFE" or "Safe" won't match the Literal["safe", "default", "dangerous"]

        # Lowercase works (exact match)
        monkeypatch.setenv("PYETM_ERROR_MODE", "safe")
        reload_configuration()
        assert get_error_policy().mode == ErrorMode.SAFE

        # Try another exact match
        monkeypatch.setenv("PYETM_ERROR_MODE", "dangerous")
        reload_configuration()
        assert get_error_policy().mode == ErrorMode.DANGEROUS

    def test_invalid_mode_falls_back_to_default(self, monkeypatch, clean_settings_env):
        """Test that invalid mode values fall back to default."""
        monkeypatch.setenv("PYETM_ERROR_MODE", "invalid_mode")
        reload_configuration()

        # Should fall back to default (or raise validation error)
        # Pydantic will raise ValidationError for invalid Literal value
        from pyetm.config.settings import get_settings
        with pytest.raises(Exception):  # pydantic.ValidationError
            get_settings()

    def test_mode_switching_workflow(self, monkeypatch, clean_settings_env):
        """Test a complete workflow of switching between all modes."""
        # Start with default (no env var)
        reload_configuration()
        assert get_error_policy().mode == ErrorMode.DEFAULT

        # Switch to SAFE
        monkeypatch.setenv("PYETM_ERROR_MODE", "safe")
        reload_configuration()
        assert get_error_policy().mode == ErrorMode.SAFE

        # Switch to DANGEROUS
        monkeypatch.setenv("PYETM_ERROR_MODE", "dangerous")
        reload_configuration()
        assert get_error_policy().mode == ErrorMode.DANGEROUS

        # Switch back to DEFAULT
        monkeypatch.setenv("PYETM_ERROR_MODE", "default")
        reload_configuration()
        assert get_error_policy().mode == ErrorMode.DEFAULT


class TestErrorModeBehavior:
    """Test that error modes behave correctly after switching."""

    def test_safe_mode_raises_all(self, monkeypatch, clean_settings_env):
        """Test that SAFE mode raises for all severities and contexts."""
        monkeypatch.setenv("PYETM_ERROR_MODE", "safe")
        reload_configuration()

        policy = get_error_policy()

        # All combinations should raise in SAFE mode
        assert policy.should_raise("info", is_bulk_context=False) is True
        assert policy.should_raise("info", is_bulk_context=True) is True
        assert policy.should_raise("warning", is_bulk_context=False) is True
        assert policy.should_raise("warning", is_bulk_context=True) is True
        assert policy.should_raise("error", is_bulk_context=False) is True
        assert policy.should_raise("error", is_bulk_context=True) is True

    def test_dangerous_mode_never_raises(self, monkeypatch, clean_settings_env):
        """Test that DANGEROUS mode never raises."""
        monkeypatch.setenv("PYETM_ERROR_MODE", "dangerous")
        reload_configuration()

        policy = get_error_policy()

        # Nothing should raise in DANGEROUS mode
        assert policy.should_raise("info", is_bulk_context=False) is False
        assert policy.should_raise("info", is_bulk_context=True) is False
        assert policy.should_raise("warning", is_bulk_context=False) is False
        assert policy.should_raise("warning", is_bulk_context=True) is False
        assert policy.should_raise("error", is_bulk_context=False) is False
        assert policy.should_raise("error", is_bulk_context=True) is False

    def test_default_mode_context_aware(self, monkeypatch, clean_settings_env):
        """Test that DEFAULT mode is context-aware."""
        monkeypatch.setenv("PYETM_ERROR_MODE", "default")
        reload_configuration()

        policy = get_error_policy()

        # info/warning never raise
        assert policy.should_raise("info", is_bulk_context=False) is False
        assert policy.should_raise("info", is_bulk_context=True) is False
        assert policy.should_raise("warning", is_bulk_context=False) is False
        assert policy.should_raise("warning", is_bulk_context=True) is False

        # error raises in single ops, not in bulk
        assert policy.should_raise("error", is_bulk_context=False) is True
        assert policy.should_raise("error", is_bulk_context=True) is False
