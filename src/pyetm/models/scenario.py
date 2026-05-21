"""Scenario model for ETM scenario management."""

from __future__ import annotations
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Union, TYPE_CHECKING, cast
from pydantic import Field, PrivateAttr
from pyetm.models.base import Base
from pyetm.clients import BaseClient
from pyetm.types import AnnualExportType, CarrierType
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

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Scenario):
            return NotImplemented
        return self.id == other.id

    def __hash__(self) -> int:
        return hash((self.id, self.area_code, self.end_year))

    @classmethod
    def create(
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
            >>> scenario = Scenario.create(
            ...     title="High Solar 2050",
            ...     area_code="nl2023",
            ...     end_year=2050
            ... )

            >>> # Save existing session
            >>> scenario = Scenario.create(
            ...     title="My Scenario",
            ...     session_id=existing_session.id
            ... )
        """
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

            session = Session.new(area_code=area_code, end_year=end_year, client=client)
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
            client = BaseClient()
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
            runner_class.run(client, scenario.session.id, data)
        except Exception as e:
            scenario.add_warning(warning_key, f"Failed to apply {warning_key}: {e}")

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
            client = BaseClient()

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

        for warning in result.errors:
            saved_scenario.add_warning("base", warning)

        return saved_scenario

    @classmethod
    def new(
        cls,
        scenario_id: int,
        title: str,
        client: Optional[BaseClient] = None,
        **kwargs: Any,
    ) -> "Scenario":
        """
        DEPRECATED: Use Scenario.create(title=..., session_id=...) instead.

        Create a new SavedScenario from an ETEngine session ID.

        Args:
            scenario_id: The ETEngine session ID to save
            title: Title for the saved scenario
            client: Optional BaseClient instance
            **kwargs: Optional params (private)

        Returns:
            SavedScenario instance

        Raises:
            SavedScenarioError: If creation fails
        """
        import warnings

        warnings.warn(
            "Scenario.new() is deprecated. "
            "Use Scenario.create(title=..., session_id=...) instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return cls.create(title=title, session_id=scenario_id, client=client, **kwargs)

    @classmethod
    def create_new(
        cls,
        title: str,
        area_code: str = "nl2023",
        end_year: int = 2050,
        client: Optional[BaseClient] = None,
        user_values: Optional[Dict[str, Any]] = None,
        custom_curves: Optional[Dict[str, Any]] = None,
        sortables: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> "Scenario":
        """
        DEPRECATED: Use Scenario.create(title=..., area_code=..., end_year=...) instead.

        Create a new ETEngine session and save it to MyETM in one step.

        Args:
            title: Title for the saved scenario
            area_code: Region code (e.g., "nl2023", "de", "uk2050"). Default: "nl2023"
            end_year: Target end year for the scenario. Default: 2050
            client: Optional BaseClient instance
            user_values: Optional dict of user input values to apply
            custom_curves: Optional dict of custom curves to upload
            sortables: Optional dict of sortables to apply
            **kwargs: Additional parameters for scenario creation (e.g., private=True)

        Returns:
            SavedScenario instance

        Raises:
            SavedScenarioError: If creation fails
        """
        import warnings

        warnings.warn(
            "Scenario.create_new() is deprecated. "
            "Use Scenario.create(title=..., area_code=..., end_year=...) instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return cls.create(
            title=title,
            area_code=area_code,
            end_year=end_year,
            client=client,
            user_values=user_values,
            custom_curves=custom_curves,
            sortables=sortables,
            **kwargs,
        )

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

        # Fetch fresh from ETEngine API
        self._scenario_session = Session.load(self.scenario_id)
        return self._scenario_session

    def update(self, client: Optional[BaseClient] = None, **kwargs: Any) -> None:
        """
        Update this SavedScenario

        Args:
            client: Optional BaseClient instance
            **kwargs: Fields to update (title, private, discarded)
        """
        if client is None:
            client = BaseClient()
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

    def custom_curve_series(self, curve_name: str) -> Optional[pd.Series[Any]]:
        """Get a custom curve series from the underlying session."""
        return self.session.custom_curve_series(curve_name)

    def custom_curves_series(self) -> Any:  # Returns generator
        """Yield all custom curve series from the underlying session."""
        return self.session.custom_curves_series()

    def get_output_curve(self, curve_name: str) -> Optional[pd.DataFrame]:
        """Get a single hourly output curve by name from the underlying session."""
        return self.session.get_output_curve(curve_name)

    def all_hourly_output_curves(self) -> Any:  # Returns generator
        """Yield all output curves from the underlying session."""
        return self.session.all_hourly_output_curves()

    def get_hourly_output_curves(
        self, carrier_type: CarrierType
    ) -> dict[str, pd.DataFrame]:
        """Get output curves by carrier type from the underlying session."""
        return self.session.get_hourly_output_curves(carrier_type)

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
            from pyetm.clients import BaseClient

            client = BaseClient()
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
        The id field contains the SavedScenario ID (MyETM ID).
        The scenario_id field contains the underlying ETEngine session ID.
        """
        # Start with Scenario specific fields
        info: Dict[str, Any] = {
            "title": self.title,
            "id": self.id,
            "private": self.private,
        }

        # Add session metadata
        session = self.session
        info.update(
            {
                "session_id": self.scenario_id,
                "preset": session.template_id,
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
            client = BaseClient()

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
            client = BaseClient()

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
            client = BaseClient()

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
