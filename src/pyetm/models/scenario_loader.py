"""Utilities for loading scenarios from various sources."""

import logging
from typing import Protocol, Optional, Dict, Any, cast
from pyetm.models.session import Session

logger = logging.getLogger(__name__)


class ScenarioLoader(Protocol):
    """
    Protocol for loading, copying, and creating scenarios.

    Different implementations interpret scenario IDs differently:
    - SessionLoader: IDs refer to ETEngine Sessions
    - SavedScenarioLoader: IDs refer to MyETM SavedScenarios
    """

    def load(
        self,
        scenario_id: int,
        area_code: Any,
        end_year: Optional[int],
        row_label: str,
        metadata_updates: Dict[str, Any],
    ) -> Optional[Session]:
        """Load an existing scenario by ID and apply metadata updates."""
        ...

    def copy(
        self,
        scenario_id: int,
        row_label: str,
        metadata_updates: Dict[str, Any],
    ) -> Optional[Session]:
        """Create a deep copy of a scenario (no template link)."""
        ...

    def create_new(
        self,
        area_code: Any,
        end_year: Optional[int],
        row_label: str,
        metadata_updates: Dict[str, Any],
    ) -> Optional[Session]:
        """Create a brand new scenario."""
        ...


class SessionLoader:
    """
    Loader for ETEngine Sessions.

    Interprets IDs as ETEngine scenario/session IDs.
    """

    def __init__(self, packer_helper: Any) -> None:
        """
        Args:
            packer_helper: Reference to ScenarioPacker instance for helper methods
        """
        self._helper = packer_helper

    def load(
        self,
        scenario_id: int,
        area_code: Any,
        end_year: Optional[int],
        row_label: str,
        metadata_updates: Dict[str, Any],
    ) -> Optional[Session]:
        """Load an ETEngine Session by ID."""
        scenario = self._helper._load_or_create_scenario(
            scenario_id, area_code, end_year, row_label, **metadata_updates
        )
        if scenario is None:
            return None
        self._helper._apply_metadata_to_scenario(scenario, metadata_updates)
        return cast(Session, scenario)

    def copy(
        self,
        scenario_id: int,
        row_label: str,
        metadata_updates: Dict[str, Any],
    ) -> Optional[Session]:
        """Deep copy an ETEngine Session."""
        try:
            source_scenario = Session.load(scenario_id)
            return source_scenario.copy(**metadata_updates)
        except Exception as e:
            logger.warning(
                "Failed to copy from Session '%s' for row '%s': %s",
                scenario_id,
                row_label,
                e,
            )
            return None

    def create_new(
        self,
        area_code: Any,
        end_year: Optional[int],
        row_label: str,
        metadata_updates: Dict[str, Any],
    ) -> Optional[Session]:
        """Create a new ETEngine Session."""
        scenario = self._helper._load_or_create_scenario(
            None, area_code, end_year, row_label, **metadata_updates
        )
        if scenario is None:
            return None
        self._helper._apply_metadata_to_scenario(scenario, metadata_updates)
        return cast(Session, scenario)


class SavedScenarioLoader:
    """
    Loader for MyETM SavedScenarios.

    Interprets IDs as MyETM SavedScenario IDs and automatically saves new scenarios.
    """

    def __init__(self, packer_helper: Any) -> None:
        """
        Args:
            packer_helper: Reference to ScenarioPacker instance for helper methods
        """
        self._helper = packer_helper

    def _require_authentication(self) -> None:
        """
        Validate that authentication token is available for SavedScenario operations.

        Raises:
            PermissionError: If no ETM_API_TOKEN is configured
        """
        from pyetm.config.settings import get_settings

        if not get_settings().etm_api_token:
            raise PermissionError(
                "SavedScenario operations require authentication. "
                "Either:\n"
                "  1. Set ETM_API_TOKEN environment variable, or\n"
                "  2. Use session=True in your Excel file for unauthenticated operations.\n"
                "Get your token at https://energytransitionmodel.com/api_access"
            )

    def load(
        self,
        scenario_id: int,
        area_code: Any,
        end_year: Optional[int],
        row_label: str,
        metadata_updates: Dict[str, Any],
    ) -> Optional[Session]:
        """Load a SavedScenario from MyETM."""
        from pyetm.models.scenario import Scenario

        try:
            saved_scenario = Scenario.load(scenario_id)
            return cast(Session, saved_scenario)
        except Exception as e:
            error_msg = str(e)
            if "does not exist" in error_msg or "not found" in error_msg.lower():
                logger.warning(
                    "Scenario %s does not exist on this ETM environment (row '%s')",
                    scenario_id,
                    row_label,
                )
            else:
                logger.warning(
                    "Could not load Scenario %s for row '%s': %s",
                    scenario_id,
                    row_label,
                    e,
                )
            return None

    def copy(
        self,
        scenario_id: int,
        row_label: str,
        metadata_updates: Dict[str, Any],
    ) -> Optional[Session]:
        """Copy a SavedScenario and save the copy to MyETM."""
        from pyetm.models.scenario import Scenario

        # Validate authentication before attempting to save
        self._require_authentication()

        try:
            saved_scenario = Scenario.load(scenario_id)
            copied_session = saved_scenario.session.copy(**metadata_updates)

            title = metadata_updates.get("title") or f"Copy of {saved_scenario.title}"
            try:
                saved_copy = copied_session.save(title=title)
                # Validate that we got a proper SavedScenario back with required attributes
                if hasattr(saved_copy, 'id') and hasattr(saved_copy, 'scenario_id'):
                    logger.info(
                        "Automatically saved copy to MyETM with ID %s (session ID: %s)",
                        saved_copy.id,
                        saved_copy.scenario_id,
                    )
                    return cast(Session, saved_copy)
                else:
                    logger.warning(
                        "Save operation for row '%s' returned unexpected object type. Returning session instead.",
                        row_label,
                    )
                    return copied_session
            except Exception as save_error:
                logger.warning(
                    "Failed to save copy to MyETM for row '%s': %s. Returning session instead.",
                    row_label,
                    save_error,
                )
                return copied_session
        except Exception as e:
            error_msg = str(e)
            if "does not exist" in error_msg or "not found" in error_msg.lower():
                logger.warning(
                    "Scenario %s does not exist on this ETM environment. Cannot copy for row '%s'",
                    scenario_id,
                    row_label,
                )
            else:
                logger.warning(
                    "Failed to copy from Scenario %s for row '%s': %s",
                    scenario_id,
                    row_label,
                    e,
                )
            return None

    def create_new(
        self,
        area_code: Any,
        end_year: Optional[int],
        row_label: str,
        metadata_updates: Dict[str, Any],
    ) -> Optional[Session]:
        """Create a new scenario and save it to MyETM."""
        from pyetm.models.scenario import Scenario

        # Validate authentication before attempting to save
        self._require_authentication()

        scenario = self._helper._load_or_create_scenario(
            None, area_code, end_year, row_label, **metadata_updates
        )
        if scenario is None:
            return None
        self._helper._apply_metadata_to_scenario(scenario, metadata_updates)

        title = metadata_updates.get("title") or f"Scenario {area_code} {end_year}"

        try:
            saved_scenario = scenario.save(title=title)
            # Validate that we got a proper SavedScenario back with required attributes
            if hasattr(saved_scenario, 'id') and hasattr(saved_scenario, 'scenario_id'):
                logger.info(
                    "Saved new scenario to MyETM with ID %s (session ID: %s)",
                    saved_scenario.id,
                    saved_scenario.scenario_id,
                )
                return cast(Session, saved_scenario)
            else:
                logger.warning(
                    "Save operation for row '%s' returned unexpected object type. Returning session instead.",
                    row_label,
                )
                return cast(Session, scenario)
        except Exception as e:
            logger.warning(
                "Failed to save new scenario to MyETM for row '%s': %s. Returning session instead.",
                row_label,
                e,
            )
            return cast(Session, scenario)
