"""Scenario model for ETM scenario management."""

from __future__ import annotations
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Union, TYPE_CHECKING, cast
from pydantic import Field, PrivateAttr
from pyetm.models.base import Base
from pyetm.clients import BaseClient, get_client
from pyetm.types import AnnualExportType
from pyetm.services.scenario_runners.create_saved_scenario import (
    CreateSavedScenarioRunner,
)
from pyetm.services.scenario_runners.update_saved_scenario import (
    UpdateSavedScenarioRunner,
)
from pyetm.services.scenario_runners.fetch_saved_scenario import (
    FetchSavedScenarioRunner,
)
from pyetm.services.scenario_runners.saved_scenario_users_index import (
    SavedScenarioUsersIndexRunner,
)
from pyetm.services.scenario_runners.saved_scenario_users_create import (
    SavedScenarioUsersCreateRunner,
)
from pyetm.services.scenario_runners.saved_scenario_users_update import (
    SavedScenarioUsersUpdateRunner,
)
from pyetm.services.scenario_runners.saved_scenario_users_destroy import (
    SavedScenarioUsersDestroyRunner,
)
from pyetm.services.scenario_runners.discard_saved_scenario import (
    DiscardSavedScenarioRunner,
)
from pyetm.services.scenario_runners.destroy_saved_scenario import (
    DestroySavedScenarioRunner,
)
import pandas as pd
from os import PathLike
from typing import Generator

if TYPE_CHECKING:
    from pyetm.models.session import Session
    from pyetm.models.inputs import Inputs
    from pyetm.models.sortables import Sortables
    from pyetm.models.custom_curves import CustomCurves
    from pyetm.models.hourly_output_curves import HourlyOutputCurves
    from pyetm.models.annual_exports import AnnualExports
    from pyetm.models.couplings import Couplings
    from pyetm.models.gqueries import Gqueries
    from pyetm.models.export_config import ExportConfig
    from pyetm.models.export_data_collection import ExportDataCollection


class SavedScenarioError(Exception):
    """Base saved scenario error"""


class Scenario(Base):
    """
    Pydantic model for a MyETM SavedScenario.

    A SavedScenario wraps an ETEngine session scenario and persists it in MyETM.
    The response includes both SavedScenario metadata and the full nested Scenario.
    """

    id: int = Field(..., description="Unique saved scenario identifier in MyETM")
    scenario_id: int = Field(..., description="Reference to ETEngine scenario")
    title: str = Field(..., description="Title of the saved scenario")
    private: Optional[bool] = False
    area_code: Optional[str] = None
    end_year: Optional[int] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    scenario: Optional[Dict[str, Any]] = None

    _scenario_session: Optional[Session] = PrivateAttr(None)
    _pending_users: Dict[str, str] = PrivateAttr(default_factory=dict)
    _client: Optional[BaseClient] = PrivateAttr(None)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Scenario):
            return NotImplemented
        return self.id == other.id

    def __hash__(self) -> int:
        return hash((self.id, self.area_code, self.end_year))

    @classmethod
    def new(
        cls,
        title: str,
        session_id: Optional[int] = None,
        area_code: Optional[str] = None,
        end_year: Optional[int] = None,
        client: Optional[BaseClient] = None,
        user_values: Optional[Dict[str, Any]] = None,
        custom_curves: Optional[Dict[str, Any]] = None,
        sortables: Optional[Dict[str, Any]] = None,
        private: bool = False,
        **kwargs: Any,
    ) -> "Scenario":
        """
        Create a SavedScenario in MyETM - either from an existing session or by creating a new one.

        Provide EITHER session_id OR (area_code + end_year), not both.

        Args:
            title: Title for the saved scenario (required)
            session_id: ID of existing Session to save (optional)
            area_code: Region code for new session, e.g., "nl2023", "de" (optional)
            end_year: End year for new session (optional)
            client: Optional BaseClient instance
            user_values: Optional dict of user input values to apply after creation
            custom_curves: Optional dict of custom curves to upload after creation
            sortables: Optional dict of sortables to apply after creation
            private: Whether the scenario should be private (default: False)
            **kwargs: Additional parameters (e.g., description)

        Returns:
            SavedScenario instance

        Raises:
            SavedScenarioError: If creation fails
            ValueError: If parameter combination is invalid

        Example:
            >>> # Create new scenario (creates new session + saves it)
            >>> scenario = Scenario.new(
            ...     title="High Solar 2050",
            ...     area_code="nl2023",
            ...     end_year=2050
            ... )

            >>> # Save existing session
            >>> scenario = Scenario.new(
            ...     title="My Scenario",
            ...     session_id=existing_session.id
            ... )
        """
        # Validate authentication
        from pyetm.config.settings import get_settings

        if not get_settings().etm_api_token:
            raise PermissionError(
                "Creating SavedScenarios requires authentication. "
                "Set ETM_API_TOKEN or create a Session instead. "
                "Get your token at https://energytransitionmodel.com/api_access"
            )

        # Validation
        if session_id is not None and (area_code is not None or end_year is not None):
            raise ValueError(
                "Provide either session_id OR (area_code + end_year), not both"
            )
        if session_id is None and (area_code is None or end_year is None):
            raise ValueError(
                "Must provide either session_id OR both area_code and end_year"
            )

        # Create new session if area_code and end_year are provided
        if area_code is not None and end_year is not None:
            from pyetm.models.session import Session

            session = Session.new(area_code=area_code, end_year=end_year)
            session_id = session.id

        # Create SavedScenario using the session_id
        params = {
            "scenario_id": session_id,
            "title": title,
            "private": private,
            **kwargs,
        }
        return cls._create_from_params(
            params, client, user_values, custom_curves, sortables
        )

    @classmethod
    def _create_from_params(
        cls,
        params: Dict[str, Any],
        client: Optional[BaseClient] = None,
        user_values: Optional[Dict[str, Any]] = None,
        custom_curves: Optional[Dict[str, Any]] = None,
        sortables: Optional[Dict[str, Any]] = None,
    ) -> "Scenario":
        """
        Internal helper to create SavedScenario from params dict.

        Args:
            params: Dictionary with required keys (scenario_id, title) and optional keys
            client: Optional BaseClient instance
            user_values: Optional dict of user input values to apply after creation
            custom_curves: Optional dict of custom curves to upload after creation
            sortables: Optional dict of sortables to apply after creation

        Returns:
            SavedScenario instance
        """
        if client is None:
            client = get_client()
        result = CreateSavedScenarioRunner.run(client, params)

        if not result.success:
            raise SavedScenarioError(
                f"Could not create saved scenario: {result.errors}"
            )

        # Validate that we received data before attempting to create the model
        if result.data is None:
            error_msg = "Could not create saved scenario: API returned no data"
            if result.errors:
                error_msg += f". Errors: {result.errors}"
            raise SavedScenarioError(error_msg)

        saved_scenario = cls.model_validate(result.data)

        # Store client for future operations
        saved_scenario._client = client

        for warning in result.errors:
            saved_scenario.add_warning("base", warning)

        for field, value in params.items():
            if hasattr(saved_scenario, field) and (
                result.data is None or field not in result.data
            ):
                setattr(saved_scenario, field, value)

        # Apply data parameters if provided
        if user_values or custom_curves or sortables:
            cls._apply_data_to_scenario(
                saved_scenario, user_values, custom_curves, sortables, client
            )

        return saved_scenario

    @staticmethod
    def _apply_submodel(
        scenario: "Scenario",
        data: Any,
        runner_class: type[Any],
        warning_key: str,
        client: BaseClient,
    ) -> None:
        """Apply a single data type to scenario with error handling."""
        try:
            result = runner_class.run(client, scenario.session, data)

            # Check if the API request failed and surface the errors
            if hasattr(result, "success") and not result.success:
                if hasattr(result, "errors") and result.errors:
                    for error in result.errors:
                        scenario.add_warning(warning_key, error)
                else:
                    scenario.add_warning(warning_key, f"Failed to apply {warning_key}")

                # Auto-display data application warnings immediately
                scenario_id_str = getattr(scenario, "id", "unknown")
                scenario_title = getattr(scenario, "title", "Unknown")
                scenario.auto_show_warnings(
                    f"SavedScenario #{scenario_id_str} (title='{scenario_title}')"
                )
        except Exception as e:
            scenario.add_warning(warning_key, f"Failed to apply {warning_key}: {e}")
            # Auto-display exception warnings immediately
            scenario_id_str = getattr(scenario, "id", "unknown")
            scenario_title = getattr(scenario, "title", "Unknown")
            scenario.auto_show_warnings(
                f"SavedScenario #{scenario_id_str} (title='{scenario_title}')"
            )

    @staticmethod
    def _apply_data_to_scenario(
        scenario: "Scenario",
        user_values: Optional[Dict[str, Any]],
        custom_curves: Optional[Dict[str, Any]],
        sortables: Optional[Dict[str, Any]],
        client: BaseClient,
    ) -> None:
        """
        Apply user_values, custom curves, and sortables to a scenario.

        Args:
            scenario: The scenario to apply data to
            user_values: Optional dict of user input values
            custom_curves: Optional dict of custom curves
            sortables: Optional dict of sortables
            client: BaseClient instance for API calls
        """
        from pyetm.services.scenario_runners.update_inputs import UpdateInputsRunner
        from pyetm.services.scenario_runners.update_custom_curves import (
            UpdateCustomCurvesRunner,
        )
        from pyetm.services.scenario_runners.update_sortables import (
            UpdateSortablesRunner,
        )

        submodels = [
            (user_values, UpdateInputsRunner, "user_values"),
            (custom_curves, UpdateCustomCurvesRunner, "custom_curves"),
            (sortables, UpdateSortablesRunner, "sortables"),
        ]

        for data, runner_class, warning_key in submodels:
            if data:
                Scenario._apply_submodel(
                    scenario, data, runner_class, warning_key, client
                )

        # Invalidate both caches so next access fetches fresh data with updates
        scenario._scenario_session = None
        scenario.scenario = None

    @classmethod
    def load(
        cls, saved_scenario_id: int, client: Optional[BaseClient] = None
    ) -> "Scenario":
        """
        Load an existing SavedScenario from MyETM by its ID.

        Args:
            saved_scenario_id: The ID of the saved scenario to load
            client: Optional BaseClient instance

        Returns:
            SavedScenario instance

        Raises:
            SavedScenarioError: If loading fails
        """
        if client is None:
            client = get_client()

        # Create a simple object with id attribute for the runner
        template = type("T", (), {"id": saved_scenario_id})()
        result = FetchSavedScenarioRunner.run(client, template)

        if not result.success:
            for error in result.errors:
                if "not found" in error.lower():
                    raise SavedScenarioError(
                        f"Scenario {saved_scenario_id} does not exist on this ETM environment"
                    )
            raise SavedScenarioError(
                f"Could not load saved scenario {saved_scenario_id}: {result.errors}"
            )

        saved_scenario = cls.model_validate(result.data)

        # Store client for future operations
        saved_scenario._client = client

        for warning in result.errors:
            saved_scenario.add_warning("base", warning)

        # Auto-display load warnings (use getattr for safety)
        scenario_id_str = getattr(saved_scenario, "id", saved_scenario_id)
        scenario_title = getattr(saved_scenario, "title", "Unknown")
        saved_scenario.auto_show_warnings(
            f"SavedScenario #{scenario_id_str} (title='{scenario_title}')"
        )

        return saved_scenario

    @property
    def session(self) -> "Session":
        """
        Get the current underlying ETEngine Scenario for this SavedScenario.

        Returns:
            Scenario: The current ETEngine scenario session (cached after first access)
        """
        from pyetm.models.session import Session

        # Return cached if already loaded
        if self._scenario_session is not None:
            return self._scenario_session

        # Build from nested data if available (e.g., from SavedScenario.load())
        if self.scenario is not None:
            self._scenario_session = Session.model_validate(self.scenario)
            return self._scenario_session

        # Fetch fresh from ETEngine API using stored client
        self._scenario_session = Session.load(self.scenario_id, client=self._client)
        return self._scenario_session

    def update(self, client: Optional[BaseClient] = None, **kwargs: Any) -> None:
        """
        Update this SavedScenario

        Args:
            client: Optional BaseClient instance
            **kwargs: Fields to update (title, private, discarded)
        """
        if client is None:
            client = get_client()
        result = UpdateSavedScenarioRunner.run(client, self.id, kwargs)

        if not result.success:
            raise SavedScenarioError(
                f"Could not update saved scenario: {result.errors}"
            )

        for warning in result.errors:
            self.add_warning("update", warning)

        if result.data:
            for field, value in result.data.items():
                if hasattr(self, field):
                    setattr(self, field, value)

        for field, value in kwargs.items():
            if hasattr(self, field) and (not result.data or field not in result.data):
                setattr(self, field, value)

    def discard(self, client: Optional[BaseClient] = None) -> None:
        """
        Discard this SavedScenario from MyETM (soft-delete, recoverable).

        The scenario is marked as discarded and hidden from listings, but can be
        recovered through the MyETM web interface within 60 days. After 60 days,
        MyETM automatically removes discarded scenarios permanently.

        This is the safe, recoverable deletion method. Use delete() for permanent deletion.

        Args:
            client: Optional BaseClient instance

        Raises:
            SavedScenarioError: If discard fails

        Example:
            scenario = Scenario.load(123)
            scenario.discard()  # Soft-delete, recoverable for 60 days
        """
        if client is None:
            client = get_client()

        result = DiscardSavedScenarioRunner.run(client, self.id)

        if not result.success:
            raise SavedScenarioError(
                f"Could not discard saved scenario: {result.errors}"
            )

    def delete(self, client: Optional[BaseClient] = None) -> None:
        """
        Permanently delete this SavedScenario AND its underlying Session (hard delete with cascade).

        WARNING: This is a PERMANENT deletion and CANNOT be undone. This will:
        1. Permanently delete the SavedScenario from MyETM
        2. Permanently delete the underlying Session from ETEngine

        All scenario data will be irreversibly lost. For recoverable deletion,
        use discard() instead.

        Args:
            client: Optional BaseClient instance

        Raises:
            SavedScenarioError: If deletion fails

        Example:
            scenario = Scenario.load(123)
            scenario.delete()  # PERMANENT deletion - cannot be recovered
        """
        if client is None:
            client = get_client()

        result = DestroySavedScenarioRunner.run(
            client,
            saved_scenario_id=self.id,
            scenario_id=self.scenario_id
        )

        if not result.success:
            raise SavedScenarioError(
                f"Could not permanently delete saved scenario: {result.errors}"
            )

    @property
    def inputs(self) -> "Inputs":
        """Get inputs from the underlying session."""
        return self.session.inputs

    @property
    def sortables(self) -> "Sortables":
        """Get sortables from the underlying session."""
        return self.session.sortables

    @property
    def custom_curves(self) -> "CustomCurves":
        """Get custom curves from the underlying session."""
        return self.session.custom_curves

    @property
    def hourly_output_curves(self) -> "HourlyOutputCurves":
        """Get output curves from the underlying session."""
        return self.session.hourly_output_curves

    @property
    def annual_exports(self) -> "AnnualExports":
        """Get annual exports from the underlying session."""
        return self.session.annual_exports

    @property
    def couplings(self) -> "Couplings":
        """Get couplings from the underlying session."""
        return self.session.couplings

    @property
    def version(self) -> str:
        """Get ETM version from the underlying session."""
        return self.session.version

    @property
    def start_year(self) -> Optional[int]:
        """Get start year from the underlying session."""
        return self.session.start_year

    @property
    def template_id(self) -> Optional[int]:
        """Get template ID from the underlying session."""
        return self.session.template_id

    @property
    def keep_compatible(self) -> Optional[bool]:
        """Get keep_compatible flag from the underlying session."""
        return self.session.keep_compatible

    @property
    def scaling(self) -> Optional[Any]:
        """Get scaling from the underlying session."""
        return self.session.scaling

    @property
    def url(self) -> Optional[str]:
        """Get URL from the underlying session."""
        return self.session.url

    def user_values(self) -> Dict[str, Any]:
        """Get user values from the underlying session."""
        return self.session.user_values()

    def update_user_values(
        self, update_inputs: Dict[str, Any], skip_upload: bool = False
    ) -> None:
        """Update user values on the underlying session."""
        self.session.update_user_values(update_inputs, skip_upload=skip_upload)

    def remove_user_values(self, input_keys: Union[List[str], Set[str]]) -> None:
        """Remove user values on the underlying session."""
        self.session.remove_user_values(input_keys)

    def set_user_values_from_dataframe(self, dataframe: pd.DataFrame) -> None:
        """Set user values from dataframe on the underlying session."""
        self.session.set_user_values_from_dataframe(dataframe)

    def update_sortables(self, update_sortables: Dict[str, List[Any]]) -> None:
        """Update sortables on the underlying session."""
        self.session.update_sortables(update_sortables)

    def remove_sortables(self, sortable_names: Union[List[str], Set[str]]) -> None:
        """Remove sortables on the underlying session."""
        self.session.remove_sortables(sortable_names)

    def set_sortables_from_dataframe(
        self, dataframe: pd.DataFrame, skip_upload: bool = False
    ) -> None:
        """Set sortables from dataframe on the underlying session."""
        self.session.set_sortables_from_dataframe(dataframe, skip_upload=skip_upload)

    def update_custom_curves(
        self, custom_curves: Any, skip_upload: bool = False
    ) -> None:
        """Update custom curves on the underlying session."""
        self.session.update_custom_curves(custom_curves, skip_upload=skip_upload)

    def remove_custom_curves(
        self, curve_keys: Union[List[str], Set[str]]
    ) -> None:
        """Remove custom curves from the underlying session."""
        self.session.remove_custom_curves(curve_keys)

    def custom_curve_series(self, curve_name: str) -> Optional[pd.Series[Any]]:
        """Get a custom curve series from the underlying session."""
        return self.session.custom_curve_series(curve_name)

    def custom_curves_series(self) -> Any:  # Returns generator
        """Yield all custom curve series from the underlying session."""
        return self.session.custom_curves_series()

    def get_hourly_curve(self, identifier: str) -> Optional[pd.DataFrame]:
        """
        Get a single hourly output curve by name or carrier type alias.

        Carrier types ('electricity', 'heat', 'hydrogen', 'methane') are treated
        as convenient aliases for their primary curves.

        Args:
            identifier: Curve name (e.g., 'merit_order') or carrier type alias

        Returns:
            DataFrame with hourly data, or None if not found
        """
        return self.session.get_hourly_curve(identifier)

    def get_hourly_curves(
        self, identifiers: list[str]
    ) -> dict[str, pd.DataFrame]:
        """
        Get multiple hourly output curves by names or carrier type aliases.

        Args:
            identifiers: List of curve names and/or carrier type aliases

        Returns:
            Dictionary mapping curve names to DataFrames
        """
        return self.session.get_hourly_curves(identifiers)

    def all_hourly_output_curves(self) -> Any:  # Returns generator
        """Yield all output curves from the underlying session."""
        return self.session.all_hourly_output_curves()

    def clear_hourly_curves_cache(self) -> int:
        """Clear all hourly output curves cache files and LRU cache.

        Returns:
            Number of files successfully removed
        """
        return self.session.clear_hourly_curves_cache()

    def clear_custom_curves_cache(self) -> int:
        """Clear all custom curves cache files.

        Returns:
            Number of files successfully removed
        """
        return self.session.clear_custom_curves_cache()

    def clear_all_curve_caches(self) -> tuple[int, int]:
        """Clear all curve caches (hourly output curves and custom curves).

        Returns:
            Tuple of (hourly_curves_removed, custom_curves_removed)
        """
        return self.session.clear_all_curve_caches()

    def clear_session_cache(self) -> None:
        """Clear entire session temp directory (all cached files).

        This removes all cached files for this session and clears
        all associated in-memory caches.
        """
        self.session.clear_session_cache()

    def get_annual_export(self, export_name: str) -> Optional[pd.DataFrame]:
        """Get a single annual export by name from the underlying session."""
        return self.session.get_annual_export(export_name)

    def get_annual_exports(
        self, export_names: AnnualExportType | list[AnnualExportType]
    ) -> dict[str, pd.DataFrame]:
        """Get multiple annual exports from the underlying session."""
        return self.session.get_annual_exports(export_names)

    def update_couplings(
        self, coupling_groups: List[str], action: str = "couple", force: bool = False
    ) -> None:
        """Update couplings on the underlying session."""
        self.session.update_couplings(coupling_groups, action, force)

    def add_queries(self, gquery_keys: Union[list[str], set[str]]) -> None:
        """Add queries to the underlying session."""
        self.session.add_queries(gquery_keys)

    def execute_queries(self) -> None:
        """Execute queries on the underlying session."""
        self.session.execute_queries()

    def results(self, columns: Any = None) -> pd.DataFrame:
        """Get query results from the underlying session."""
        return self.session.results(columns)

    def queries_requested(self) -> bool:
        """Check if queries have been requested on the underlying session."""
        return self.session.queries_requested()

    def set_export_config(self, config: "ExportConfig" | None) -> None:
        """Set export config on the underlying session."""
        self.session.set_export_config(config)

    def get_export_config(self) -> "ExportConfig" | None:
        """Get export config from the underlying session."""
        return self.session.get_export_config()

    def show_all_warnings(self) -> None:
        """Show all warnings from the underlying session."""
        self.session.show_all_warnings()

    def identifier(self) -> Union[str, int]:
        """Get identifier in priority order: saved title, short_name, session title, saved id, session id."""
        if self.title:
            return self.title
        if self.session.short_name:
            return self.session.short_name
        if self.session.title:
            return self.session.title
        if self.id:
            return self.id
        return self.session.id

    def set_short_name(self, short_name: str) -> None:
        """Set short name on the underlying session."""
        self.session.set_short_name(short_name)

    def update_metadata(self, **kwargs: Any) -> Optional[Dict[str, Any]]:
        """Update metadata on the underlying session."""
        return self.session.update_metadata(**kwargs)

    def copy_with_preset(self, **overrides: Any) -> "Scenario":
        """
        Create a copy of the underlying session with a linked preset and save it to MyETM.
        """
        # Separate SavedScenario parameters from Session copy parameters
        title = overrides.pop("title", f"Copy of {self.title}")
        private = overrides.pop("private", None)

        # Copy the underlying session with preset link, passing session-related overrides
        copied_session = self.session.copy_with_preset(**overrides)

        # Save the copied session to MyETM as a new SavedScenario
        save_params = {"title": title}
        if private is not None:
            save_params["private"] = private

        return cast("Scenario", copied_session.save(**save_params))

    def copy(
        self,
        user_values: Optional[Dict[str, Any]] = None,
        custom_curves: Optional[Dict[str, Any]] = None,
        sortables: Optional[Dict[str, Any]] = None,
        **overrides: Any,
    ) -> "Scenario":
        """
        Create a copy with no template link to the original scenario and save it to MyETM.

        Args:
            user_values: Optional dict of user input values to apply after copying
            custom_curves: Optional dict of custom curves to upload after copying
            sortables: Optional dict of sortables to apply after copying
            **overrides: Additional parameters to override (title, private, etc.)

        Returns:
            Copied SavedScenario instance
        """
        # Separate SavedScenario parameters from Session copy parameters
        title = overrides.pop("title", f"Copy of {self.title}")
        private = overrides.pop("private", None)

        # Copy the underlying session (no preset link), passing session-related overrides
        copied_session = self.session.copy(**overrides)

        # Save the copied session to MyETM as a new SavedScenario
        save_params = {"title": title}
        if private is not None:
            save_params["private"] = private

        copied_scenario = copied_session.save(**save_params)

        # Apply data parameters if provided
        if user_values or custom_curves or sortables:
            from pyetm.clients import BaseClient, get_client

            client = get_client()
            Scenario._apply_data_to_scenario(
                copied_scenario, user_values, custom_curves, sortables, client
            )

        return cast("Scenario", copied_scenario)

    @classmethod
    def interpolate(
        cls,
        scenarios: Union["Scenario", List["Scenario"]],
        *end_years: int,
        titles: Optional[List[str]] = None,
        client: Optional[BaseClient] = None,
        **kwargs: Any,
    ) -> List["Scenario"]:
        """
        Interpolate one or more saved scenarios to target years and save to MyETM.
        """
        end_years_list = list(end_years)

        if titles is not None and len(titles) != len(end_years_list):
            raise ValueError(
                f"Length of titles ({len(titles)}) must match length of "
                f"end_years ({len(end_years_list)})"
            )

        # Get underlying sessions and perform interpolation
        from pyetm.models.session import Session

        scenario_list = scenarios if isinstance(scenarios, list) else [scenarios]
        sessions = [sc.session for sc in scenario_list]
        interpolated_sessions = Session.interpolate(sessions, *end_years, client=client)

        # Save each interpolated session as a SavedScenario
        saved_scenarios_list = []
        for i, session in enumerate(interpolated_sessions):
            # Generate title if not provided
            if titles:
                title = titles[i]
            else:
                title = f"Interpolated to {session.end_year}"

            saved = session.save(client=client, title=title, **kwargs)
            saved_scenarios_list.append(saved)

        return saved_scenarios_list

    def to_excel(self, path: PathLike[str] | str, **export_options: Any) -> None:
        """Export this saved scenario to Excel."""
        self.session.to_excel(path, **export_options)

    def collect_export_data(self, **export_options: Any) -> "ExportDataCollection":
        """
        Returns ExportDataCollection containing pandas DataFrames and dictionaries
        that can be exported to any file format (Parquet, CSV, JSON, etc.).
        """
        return self.session.collect_export_data(**export_options)

    def _to_dataframe(self, **kwargs: Any) -> "pd.DataFrame":
        """
        Return a single-column DataFrame describing this saved scenario.

        Exports SavedScenario metadata merged with underlying session data.
        The saved_scenario_id field contains the SavedScenario ID (MyETM ID).
        The session_id field contains the underlying ETEngine session ID.
        """
        # Start with Scenario specific fields
        info: Dict[str, Any] = {
            "title": self.title,
            "saved_scenario_id": self.id,
            "session_id": self.scenario_id,
            "private": self.private,
        }

        # Add session metadata
        session = self.session
        info.update(
            {
                "area_code": session.area_code,
                "start_year": session.start_year,
                "end_year": session.end_year,
                "keep_compatible": session.keep_compatible,
                "source": session.source,
                "url": session.url,
                "version": session.version,
                "created_at": session.created_at,
                "updated_at": session.updated_at,
            }
        )

        # Add short_name if available
        if session.short_name:
            info["short_name"] = session.short_name

        # Flatten session metadata keys
        if session.metadata and isinstance(session.metadata, dict):
            for k, v in session.metadata.items():
                if k not in info:
                    info[k] = v

        col_name = str(self.id)
        return pd.DataFrame.from_dict(info, orient="index", columns=[col_name])

    def list_users(self, client: Optional[BaseClient] = None) -> List[Dict[str, Any]]:
        """
        Fetch all users with access to this saved scenario.
        """
        if client is None:
            client = get_client()

        result = SavedScenarioUsersIndexRunner.run(client, self.id)

        if not result.success:
            raise SavedScenarioError(f"Could not fetch users: {result.errors}")

        if result.data is None:
            return []

        for user in result.data:
            user["role"] = user["role"].replace("scenario_", "", 1)

        return result.data

    def update_users(
        self,
        email: str,
        role: str,
        client: Optional[BaseClient] = None,
        skip_upload: bool = False,
    ) -> None:
        """
        Add, update, or remove a user's access to this saved scenario.
        - skip_upload: If True, store data locally without uploading (can be applied later)
        """
        if client is None:
            client = get_client()

        role = self._normalize_role(role)

        if skip_upload:
            self._pending_users[email] = role
            return

        if role == "remove":
            self._remove_user(email, client)
            return

        if self._user_exists(email, client):
            self._update_user_role(email, role, client)
        else:
            self._add_user(email, role, client)

    def apply_pending_users(self, client: Optional[BaseClient] = None) -> int:
        """
        Apply all pending user updates that were loaded with skip_upload=True.
        """
        if not self._pending_users:
            return 0

        if client is None:
            client = get_client()

        count = 0
        for email, role in list(self._pending_users.items()):
            try:
                self.update_users(email, role, client=client, skip_upload=False)
                count += 1
            except Exception as e:
                from pyetm.models.scenario import SavedScenarioError
                import logging

                logging.getLogger(__name__).warning(
                    "Failed to apply pending user '%s' with role '%s': %s",
                    email,
                    role,
                    e,
                )

        # Clear pending users after applying
        self._pending_users.clear()
        return count

    def _normalize_role(self, role: str) -> str:
        role_aliases = {
            "scenario_owner": {"owner", "scenario_owner"},
            "scenario_collaborator": {"collaborator", "scenario_collaborator"},
            "scenario_viewer": {"viewer", "scenario_viewer"},
            "remove": {"remove"},
        }
        role_lower = role.lower() if isinstance(role, str) else None
        normalized_role = next(
            (k for k, v in role_aliases.items() if role_lower in v), None
        )
        if not normalized_role:
            valid_roles = ", ".join(role_aliases.keys())
            raise ValueError(f"Invalid role: {role}. Must be one of: {valid_roles}")
        return normalized_role

    def _user_exists(self, email: str, client: BaseClient) -> bool:
        return any(u.get("user_email") == email for u in self.list_users(client))

    def _remove_user(self, email: str, client: BaseClient) -> None:
        result = SavedScenarioUsersDestroyRunner.run(
            client, self.id, [{"user_email": email}]
        )
        if not result.success:
            raise SavedScenarioError(f"Could not remove user: {result.errors}")

    def _update_user_role(self, email: str, role: str, client: BaseClient) -> None:
        result = SavedScenarioUsersUpdateRunner.run(
            client, self.id, [{"user_email": email, "role": role}]
        )
        if not result.success:
            raise SavedScenarioError(f"Could not update user: {result.errors}")

    def _add_user(self, email: str, role: str, client: BaseClient) -> None:
        result = SavedScenarioUsersCreateRunner.run(
            client, self.id, [{"user_email": email, "role": role}]
        )
        if not result.success:
            raise SavedScenarioError(f"Could not add user: {result.errors}")
