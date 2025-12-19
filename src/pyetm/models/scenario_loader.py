import logging
from typing import Protocol, Optional, Dict, Any
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

    def __init__(self, packer_helper):
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
        return scenario

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
        return scenario


class SavedScenarioLoader:
    """
    Loader for MyETM SavedScenarios.

    Interprets IDs as MyETM SavedScenario IDs and automatically saves new scenarios.
    """

    def __init__(self, packer_helper):
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
        """Load a SavedScenario from MyETM."""
        from pyetm.models.scenario import Scenario

        try:
            saved_scenario = Scenario.load(scenario_id)
            return saved_scenario
        except Exception as e:
            logger.warning(
                "Could not load SavedScenario %s for row '%s': %s",
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

        try:
            saved_scenario = Scenario.load(scenario_id)
            copied_session = saved_scenario.session.copy(**metadata_updates)

            title = metadata_updates.get("title") or f"Copy of {saved_scenario.title}"
            try:
                saved_copy = copied_session.save(title=title)
                logger.info(
                    "Automatically saved copy to MyETM with ID %s (session ID: %s)",
                    saved_copy.id,
                    saved_copy.scenario_id,
                )
                return saved_copy
            except Exception as save_error:
                logger.warning(
                    "Failed to save copy to MyETM for row '%s': %s. Returning session instead.",
                    row_label,
                    save_error,
                )
                return copied_session
        except Exception as e:
            logger.warning(
                "Failed to copy from SavedScenario '%s' for row '%s': %s",
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

        scenario = self._helper._load_or_create_scenario(
            None, area_code, end_year, row_label, **metadata_updates
        )
        if scenario is None:
            return None
        self._helper._apply_metadata_to_scenario(scenario, metadata_updates)

        title = metadata_updates.get("title") or f"Scenario {area_code} {end_year}"

        try:
            saved_scenario = scenario.save(title=title)
            logger.info(
                "Automatically saved new scenario to MyETM with ID %s (session ID: %s)",
                saved_scenario.id,
                saved_scenario.scenario_id,
            )
            return saved_scenario
        except Exception as e:
            logger.warning(
                "Failed to save new scenario to MyETM for row '%s': %s. Returning session instead.",
                row_label,
                e,
            )
            return scenario
