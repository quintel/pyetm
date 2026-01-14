from __future__ import annotations
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Union, TYPE_CHECKING
from pydantic import Field, PrivateAttr
from pyetm.models.base import Base
from pyetm.clients import BaseClient
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

if TYPE_CHECKING:
    from pyetm.models.session import Session
    from pyetm.models.inputs import Inputs
    from pyetm.models.sortables import Sortables
    from pyetm.models.custom_curves import CustomCurves
    from pyetm.models.output_curves import OutputCurves
    from pyetm.models.couplings import Couplings
    from pyetm.models.gqueries import Gqueries
    from pyetm.models.export_config import ExportConfig


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
    description: Optional[str] = None
    private: Optional[bool] = False
    area_code: Optional[str] = None
    end_year: Optional[int] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    scenario: Optional[Dict[str, Any]] = None

    _scenario_session: Optional[Session] = PrivateAttr(None)

    def __eq__(self, other: "Scenario"):
        return self.id == other.id

    def __hash__(self):
        return hash((self.id, self.area_code, self.end_year))

    @classmethod
    def create(
        cls, params: Dict[str, Any], client: Optional[BaseClient] = None
    ) -> "Scenario":
        """
        Create a new SavedScenario in MyETM from an existing session scenario.

        Args:
            params: Dictionary with required keys (scenario_id, title) and optional keys
                   (private)
            client: Optional BaseClient instance

        Returns:
            SavedScenario instance

        Raises:
            SavedScenarioError if creation fails
        """
        if client is None:
            client = BaseClient()
        result = CreateSavedScenarioRunner.run(client, params)

        if not result.success:
            raise SavedScenarioError(
                f"Could not create saved scenario: {result.errors}"
            )

        saved_scenario = cls.model_validate(result.data)

        for warning in result.errors:
            saved_scenario.add_warning("base", warning)

        for field, value in params.items():
            if hasattr(saved_scenario, field) and field not in result.data:
                setattr(saved_scenario, field, value)

        return saved_scenario

    @classmethod
    def from_scenario(
        cls,
        scenario: "Session",
        title: str,
        client: Optional[BaseClient] = None,
        **kwargs,
    ) -> "Scenario":
        """
        Convenience method to create SavedScenario from a Scenario instance.

        Args:
            scenario: Scenario instance to save
            title: Title for the saved scenario
            client: Optional BaseClient instance
            **kwargs: Optional params (private)

        Returns:
            SavedScenario instance
        """
        params = {"scenario_id": scenario.id, "title": title, **kwargs}
        return cls.create(params, client=client)

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
            SavedScenarioError if loading fails
        """
        if client is None:
            client = BaseClient()

        template = type("T", (), {"id": saved_scenario_id})
        result = FetchSavedScenarioRunner.run(client, template)

        if not result.success:
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
        **kwargs,
    ) -> "Scenario":
        """
        Create a new SavedScenario from an ETEngine scenario ID.

        Args:
            scenario_id: The ETEngine scenario ID to save
            title: Title for the saved scenario
            client: Optional BaseClient instance
            **kwargs: Optional params (private)

        Returns:
            SavedScenario instance

        Raises:
            SavedScenarioError if creation fails
        """
        params = {"scenario_id": scenario_id, "title": title, **kwargs}
        return cls.create(params, client=client)

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

    def update(self, client: Optional[BaseClient] = None, **kwargs) -> None:
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
    def output_curves(self) -> "OutputCurves":
        """Get output curves from the underlying session."""
        return self.session.output_curves

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
    def template(self) -> Optional[int]:
        """Get template ID from the underlying session."""
        return self.session.template

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

    def update_custom_curves(self, custom_curves, skip_upload: bool = False) -> None:
        """Update custom curves on the underlying session."""
        self.session.update_custom_curves(custom_curves, skip_upload=skip_upload)

    def custom_curve_series(self, curve_name: str) -> pd.Series:
        """Get a custom curve series from the underlying session."""
        return self.session.custom_curve_series(curve_name)

    def custom_curves_series(self):
        """Yield all custom curve series from the underlying session."""
        return self.session.custom_curves_series()

    def output_curve(self, curve_name: str) -> pd.DataFrame:
        """Get an output curve from the underlying session."""
        return self.session.output_curve(curve_name)

    def all_output_curves(self):
        """Yield all output curves from the underlying session."""
        return self.session.all_output_curves()

    def get_output_curves(self, carrier_type: str) -> dict[str, pd.DataFrame]:
        """Get output curves by carrier type from the underlying session."""
        return self.session.get_output_curves(carrier_type)

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

    def results(self, columns=None) -> pd.DataFrame:
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
        """Get identifier (short_name, title, or id) from the underlying session."""
        return self.session.identifier()

    def set_short_name(self, short_name: str) -> None:
        """Set short name on the underlying session."""
        self.session.set_short_name(short_name)

    def update_metadata(self, **kwargs) -> Dict[str, Any]:
        """Update metadata on the underlying session."""
        return self.session.update_metadata(**kwargs)

    def copy(self, **overrides) -> "Session":
        """Create a copy of the underlying session."""
        return self.session.copy(**overrides)

    def deep_copy(self, **overrides) -> "Session":
        """Create a deep copy of the underlying session."""
        return self.session.deep_copy(**overrides)

    @classmethod
    def interpolate(
        cls,
        scenarios: Union["Scenario", List["Scenario"]],
        *end_years: int,
        titles: Optional[List[str]] = None,
        client: Optional[BaseClient] = None,
        **kwargs,
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

    def to_excel(self, path: PathLike | str, **export_options) -> None:
        """Export this saved scenario to Excel."""
        self.session.to_excel(path, **export_options)

    def _to_dataframe(self, **kwargs) -> "pd.DataFrame":
        """
        Return a single-column DataFrame describing this saved scenario.

        Exports SavedScenario metadata merged with underlying session data.
        The scenario_id field contains the SavedScenario ID (MyETM ID), not the session ID.
        """
        # Start with SavedScenario-specific fields
        info: Dict[str, Any] = {
            "title": self.title,
            "description": self.description,
            "scenario_id": self.id,  # MyETM SavedScenario ID
            "private": self.private,
        }

        # Add session metadata
        session = self.session
        info.update(
            {
                "template": session.template,
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

        col_name = self.identifier() if self.identifier() is not None else self.id
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

        for user in result.data:
            user['role'] = user['role'].replace('scenario_', '', 1)

        return result.data

    def update_users(
        self, email: str, role: str, client: Optional[BaseClient] = None
    ) -> None:
        """
        Add, update, or remove a user's access to this saved scenario.
        """
        if client is None:
            client = BaseClient()

        role = self._normalize_role(role)

        if role == "remove":
            self._remove_user(email, client)
            return

        if self._user_exists(email, client):
            self._update_user_role(email, role, client)
        else:
            self._add_user(email, role, client)

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
