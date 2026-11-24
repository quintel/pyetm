"""Scenario model: a saved scenario in the ETM, and everything you can do with it."""

from __future__ import annotations

import logging
import shutil
from datetime import datetime
from os import PathLike
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Set, Union, cast

import pandas as pd
from pydantic import ConfigDict, Field, PrivateAttr

from pyetm.clients import BaseClient, get_client
from pyetm.config.settings import get_settings
from pyetm.models.annual_exports import AnnualExports
from pyetm.models.base import Base
from pyetm.models.couplings import Couplings
from pyetm.models.custom_curves import CustomCurves
from pyetm.models.export_config import ExportConfig
from pyetm.models.gqueries import Gqueries
from pyetm.models.hourly_output_curves import HourlyOutputCurves
from pyetm.models.inputs import Inputs
from pyetm.models.sortables import Sortables
from pyetm.services.scenario_runners.base_runner import BaseRunner
from pyetm.services.scenario_runners.break_preset_link import BreakPresetLinkRunner
from pyetm.services.scenario_runners.create_saved_scenario import CreateSavedScenarioRunner
from pyetm.services.scenario_runners.delete_custom_curves import DeleteCustomCurvesRunner
from pyetm.services.scenario_runners.destroy_saved_scenario import DestroySavedScenarioRunner
from pyetm.services.scenario_runners.discard_saved_scenario import DiscardSavedScenarioRunner
from pyetm.services.scenario_runners.fetch_couplings import FetchCouplingsRunner
from pyetm.services.scenario_runners.fetch_custom_curves import FetchAllCustomCurveDataRunner
from pyetm.services.scenario_runners.fetch_inputs import FetchInputsRunner
from pyetm.services.scenario_runners.fetch_metadata import FetchMetadataRunner
from pyetm.services.scenario_runners.fetch_saved_scenario import FetchSavedScenarioRunner
from pyetm.services.scenario_runners.fetch_sortables import FetchSortablesRunner
from pyetm.services.scenario_runners.save_snapshot import SaveSnapshotRunner
from pyetm.services.scenario_runners.saved_scenario_users_create import (
    SavedScenarioUsersCreateRunner,
)
from pyetm.services.scenario_runners.saved_scenario_users_destroy import (
    SavedScenarioUsersDestroyRunner,
)
from pyetm.services.scenario_runners.saved_scenario_users_index import (
    SavedScenarioUsersIndexRunner,
)
from pyetm.services.scenario_runners.saved_scenario_users_update import (
    SavedScenarioUsersUpdateRunner,
)
from pyetm.services.scenario_runners.update_couplings import UpdateCouplingsRunner
from pyetm.services.scenario_runners.update_custom_curves import UpdateCustomCurvesRunner
from pyetm.services.scenario_runners.update_inputs import UpdateInputsRunner
from pyetm.services.scenario_runners.update_metadata import UpdateMetadataRunner
from pyetm.services.scenario_runners.update_saved_scenario import UpdateSavedScenarioRunner
from pyetm.services.scenario_runners.update_sortables import UpdateSortablesRunner
from pyetm.types import AnnualExportType
from pyetm.validators import validate_export_names

if TYPE_CHECKING:
    from pyetm.models.export_data_collection import ExportDataCollection

logger = logging.getLogger(__name__)

ROLES = ("scenario_owner", "scenario_collaborator", "scenario_viewer")

# The Versions already warned about, so loading many Scenarios from one warns once
_versions_warned: Set[str] = set()


class ScenarioError(Exception):
    """A Scenario could not be created, loaded or changed."""


# The 2.0 name, kept so existing `except SavedScenarioError` handlers still catch.
SavedScenarioError = ScenarioError


class Scenario(Base):
    """
    A saved scenario in the ETM.

    It has a title, privacy, users and a history of saved snapshots, and every input, curve,
    query and export call works on its current state.
    """

    model_config = ConfigDict(validate_assignment=True, populate_by_name=True)

    id: Optional[int] = Field(None, description="The saved scenario's ID")
    title: Optional[str] = None
    description: Optional[str] = None
    scenario_id: Optional[int] = Field(None, description="The current Session, for reference only")
    area_code: Optional[str] = None
    end_year: Optional[int] = None
    version: Optional[str] = Field(None, description="The Version the scenario is on")
    private: Optional[bool] = None
    discarded_at: Optional[datetime] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    short_name: Optional[str] = Field(None, description="A local label for workbooks and exports")

    _client: Optional[BaseClient] = PrivateAttr(None)
    _session: Optional[Dict[str, Any]] = PrivateAttr(None)
    _pending_users: Dict[str, str] = PrivateAttr(default_factory=dict)
    _inputs: Optional[Inputs] = PrivateAttr(None)
    _sortables: Optional[Sortables] = PrivateAttr(None)
    _custom_curves: Optional[CustomCurves] = PrivateAttr(None)
    _hourly_output_curves: Optional[HourlyOutputCurves] = PrivateAttr(None)
    _annual_exports: Optional[AnnualExports] = PrivateAttr(None)
    _queries: Optional[Gqueries] = PrivateAttr(None)
    _export_config: Optional[ExportConfig] = PrivateAttr(None)
    _couplings: Optional[Couplings] = PrivateAttr(None)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Scenario):
            return NotImplemented
        return self.id == other.id

    def __hash__(self) -> int:
        return hash((self.id, self.area_code, self.end_year))

    # --- Creating and loading ----------------------------------------------------------------

    @classmethod
    def new(
        cls,
        title: str,
        area_code: str,
        end_year: int,
        private: Optional[bool] = None,
        description: Optional[str] = None,
        client: Optional[BaseClient] = None,
        user_values: Optional[Dict[str, Any]] = None,
        custom_curves: Optional[Dict[str, Any]] = None,
        sortables: Optional[Dict[str, Any]] = None,
    ) -> "Scenario":
        """
        Create a scenario from scratch on ETM_VERSION.

        Example:
            >>> scenario = Scenario.new(title="High Solar 2050", area_code="nl2023", end_year=2050)
        """
        params = {"title": title, "area_code": area_code, "end_year": end_year, "private": private}
        params.update(description=description, version=get_settings().etm_version)
        return cls._create(params, client, user_values, custom_curves, sortables)

    @classmethod
    def _create(
        cls,
        params: Dict[str, Any],
        client: Optional[BaseClient] = None,
        user_values: Optional[Dict[str, Any]] = None,
        custom_curves: Optional[Dict[str, Any]] = None,
        sortables: Optional[Dict[str, Any]] = None,
    ) -> "Scenario":
        """Create through MyETM, then apply any inputs, curves and sortables to the new Session."""
        client = client or get_client()
        sent = {key: value for key, value in params.items() if value is not None}
        result = CreateSavedScenarioRunner.run(client, sent)
        if not result.success:
            raise ScenarioError(f"Could not create saved scenario: {result.errors}")

        scenario = cls._from_myetm(cast(Dict[str, Any], result.data), client, result.errors)
        scenario._apply_data(user_values, custom_curves, sortables)
        return scenario

    @classmethod
    def _from_myetm(
        cls, data: Dict[str, Any], client: BaseClient, warnings: Optional[List[str]] = None
    ) -> "Scenario":
        scenario = cls.model_validate(data)
        scenario._client = client
        for warning in warnings or []:
            scenario.add_warning("base", warning)
        scenario._warn_on_other_version()
        return scenario

    def _warn_on_other_version(self) -> None:
        """pyETM targets ETM_VERSION. Another Version may work, so it warns once and carries on."""
        target = get_settings().etm_version
        if not self.version or self.version == target or self.version in _versions_warned:
            return
        _versions_warned.add(self.version)
        logger.warning(
            "Scenario %s is on Version %s, but this pyETM release targets %s. "
            "It may still work, but is not tested against it.",
            self.id,
            self.version,
            target,
        )

    @classmethod
    def load(cls, saved_scenario_id: int, client: Optional[BaseClient] = None) -> "Scenario":
        """Load a saved scenario by its ID."""
        client = client or get_client()
        result = FetchSavedScenarioRunner.run(client, saved_scenario_id)
        if not result.success:
            raise ScenarioError(f"Could not load saved scenario {saved_scenario_id}: {result.errors}")

        scenario = cls._from_myetm(cast(Dict[str, Any], result.data), client, result.errors)
        scenario.auto_show_warnings(f"Scenario #{scenario.id} (title='{scenario.title}')")
        return scenario

    @classmethod
    def from_excel(cls, xlsx_path: PathLike[str] | str) -> "Scenario":
        """Load a single scenario from Excel."""
        from pyetm.models.scenario_packer import ScenarioPacker

        scenarios = list(ScenarioPacker.from_excel(xlsx_path)._scenarios())
        if len(scenarios) != 1:
            raise ScenarioError(
                f"Expected one scenario, found {len(scenarios)}. "
                "Use Scenarios.from_excel() for multi-scenario workbooks."
            )
        return scenarios[0]

    @property
    def client(self) -> BaseClient:
        """The client this scenario was loaded with, or the default one."""
        return self._client or get_client()

    # --- Copying, interpolating and saving -----------------------------------------------------

    def copy_with_preset(
        self, title: Optional[str] = None, private: Optional[bool] = None
    ) -> "Scenario":
        """
        Copy this scenario into a new one whose preset is this scenario's current Session, so
        its edits read against this one.
        """
        params = {"title": title or f"Copy of {self.title}", "source": self.id}
        params.update(version=self.version, private=private)
        return self._create(params, self._client)

    def copy(  # type: ignore[override]
        self,
        title: Optional[str] = None,
        private: Optional[bool] = None,
        user_values: Optional[Dict[str, Any]] = None,
        custom_curves: Optional[Dict[str, Any]] = None,
        sortables: Optional[Dict[str, Any]] = None,
    ) -> "Scenario":
        """Copy this scenario into an independent one, with no preset link back to this one."""
        copied = self.copy_with_preset(title=title, private=private)
        result = BreakPresetLinkRunner.run(copied.client, copied)
        if not result.success:
            raise ScenarioError(f"Copied scenario {copied.id} but could not break its preset link: {result.errors}")

        copied._apply_data(user_values, custom_curves, sortables)
        return copied

    @classmethod
    def interpolate(
        cls,
        scenarios: Union["Scenario", List["Scenario"]],
        *end_years: int,
        titles: Optional[List[str]] = None,
        client: Optional[BaseClient] = None,
    ) -> List["Scenario"]:
        """
        Create a scenario for each earlier end year, with inputs interpolated from this source.

        Example:
            >>> Scenario.interpolate(scenario_2050, 2030, 2040)
        """
        source = cls._single_source(scenarios)
        if titles is not None and len(titles) != len(end_years):
            raise ValueError(f"Length of titles ({len(titles)}) must match length of end_years ({len(end_years)})")

        titles = titles or [f"{source.title} interpolated to {year}" for year in end_years]
        return [
            cls._create(
                {"title": title, "source": source.id, "end_year": year, "version": source.version},
                client or source._client,
            )
            for title, year in zip(titles, end_years)
        ]

    @staticmethod
    def _single_source(scenarios: Union["Scenario", List["Scenario"]]) -> "Scenario":
        if not isinstance(scenarios, list):
            return scenarios
        if len(scenarios) == 1:
            return scenarios[0]
        raise ValueError(
            "Interpolating between several scenarios is not supported. "
            "Interpolate from one source scenario at a time."
        )

    def save(self) -> None:
        """
        Save this scenario: its current Session becomes a Snapshot in its history, and a copy of it
        becomes current. Changes made since the last save are kept either way.
        """
        result = SaveSnapshotRunner.run(self.client, cast(int, self.id))
        if not result.success:
            raise ScenarioError(f"Could not save scenario {self.id}: {result.errors}")
        self._refresh(cast(Dict[str, Any], result.data))

    def _refresh(self, data: Dict[str, Any]) -> None:
        for field, value in data.items():
            if field in Scenario.model_fields:
                setattr(self, field, value)

    # --- The MyETM record ---------------------------------------------------------------------

    def update(self, **kwargs: Any) -> None:
        """
        Update this scenario's title, description or privacy.

        Example:
            >>> scenario.update(title="Renamed", private=True)
        """
        result = UpdateSavedScenarioRunner.run(self.client, cast(int, self.id), kwargs)
        if not result.success:
            raise ScenarioError(f"Could not update saved scenario: {result.errors}")

        for warning in result.errors:
            self.add_warning("update", warning)
        self._refresh(cast(Dict[str, Any], result.data))

    def discard(self) -> None:
        """
        Discard this scenario. The ETM hides it from listings and keeps it recoverable through its
        web interface for 60 days, then deletes it. Use delete() to delete it at once.
        """
        result = DiscardSavedScenarioRunner.run(self.client, cast(int, self.id))
        if not result.success:
            raise ScenarioError(f"Could not discard saved scenario: {result.errors}")
        self._refresh(cast(Dict[str, Any], result.data))

    def delete(self) -> None:
        """Permanently delete this scenario. This cannot be undone."""
        result = DestroySavedScenarioRunner.run(self.client, cast(int, self.id))
        if not result.success:
            raise ScenarioError(f"Could not permanently delete saved scenario: {result.errors}")

    # --- Users --------------------------------------------------------------------------------

    def list_users(self) -> List[Dict[str, Any]]:
        """Every user with access, as {"user_email", "role", "pending"} with a short role name."""
        result = SavedScenarioUsersIndexRunner.run(self.client, cast(int, self.id))
        if not result.success:
            raise ScenarioError(f"Could not fetch users: {result.errors}")

        return [
            {
                "user_email": user["email"],
                "role": user["role"].replace("scenario_", "", 1),
                "pending": user.get("pending", False),
            }
            for user in result.data or []
        ]

    def update_users(self, email: str, role: str, skip_upload: bool = False) -> None:
        """
        Add a user, change their role, or remove them with role "remove".

        skip_upload keeps the change locally until apply_pending_users() is called.
        """
        role = self._normalize_role(role)
        if skip_upload:
            self._pending_users[email] = role
            return

        if role == "remove":
            self._change_users(SavedScenarioUsersDestroyRunner, {"email": email}, "remove")
        elif any(user["user_email"] == email for user in self.list_users()):
            self._change_users(SavedScenarioUsersUpdateRunner, {"email": email, "role": role}, "update")
        else:
            self._change_users(SavedScenarioUsersCreateRunner, {"email": email, "role": role}, "add")

    def _change_users(self, runner: Any, user: Dict[str, str], action: str) -> None:
        result = runner.run(self.client, cast(int, self.id), [user])
        if not result.success:
            raise ScenarioError(f"Could not {action} user: {result.errors}")

    def apply_pending_users(self) -> int:
        """Apply the user changes kept with skip_upload=True. Returns how many were applied."""
        applied = 0
        for email, role in list(self._pending_users.items()):
            try:
                self.update_users(email, role)
                applied += 1
            except ScenarioError as e:
                logger.warning("Failed to apply pending user '%s' with role '%s': %s", email, role, e)

        self._pending_users.clear()
        return applied

    @staticmethod
    def _normalize_role(role: str) -> str:
        """Accept a role with or without its scenario_ prefix, or "remove"."""
        name = role.lower() if isinstance(role, str) else ""
        if name == "remove":
            return name
        if f"scenario_{name}" in ROLES:
            return f"scenario_{name}"
        if name in ROLES:
            return name
        raise ValueError(f"Invalid role: {role}. Must be one of: {', '.join(ROLES)}, remove")

    # --- The current Session's metadata -------------------------------------------------------

    def _session_metadata(self) -> Dict[str, Any]:
        """The current Session's own fields, fetched through MyETM on first use."""
        if self._session is None:
            result = FetchMetadataRunner.run(self.client, self)
            if not result.success:
                raise ScenarioError(f"Could not load scenario {self.id} from the engine: {result.errors}")
            self._session = cast(Dict[str, Any], result.data)
        return self._session

    @property
    def start_year(self) -> Optional[int]:
        return self._session_metadata().get("start_year")

    @property
    def keep_compatible(self) -> Optional[bool]:
        return self._session_metadata().get("keep_compatible")

    @property
    def template_id(self) -> Optional[int]:
        """The Session this scenario's current Session was copied from, if any."""
        session = self._session_metadata()
        return session.get("template", session.get("preset_scenario_id"))

    @property
    def scaling(self) -> Optional[Any]:
        return self._session_metadata().get("scaling")

    @property
    def source(self) -> Optional[str]:
        return self._session_metadata().get("source")

    @property
    def url(self) -> Optional[str]:
        return self._session_metadata().get("url")

    @property
    def metadata(self) -> Dict[str, Any]:
        """The current Session's free-form metadata."""
        session = self._session_metadata()
        if not isinstance(session.get("metadata"), dict):
            session["metadata"] = {}
        return cast(Dict[str, Any], session["metadata"])

    def update_metadata(self, **kwargs: Any) -> Optional[Dict[str, Any]]:
        """Update the current Session's own fields, such as keep_compatible, source or metadata."""
        result = UpdateMetadataRunner.run(self.client, self, kwargs)
        if not result.success:
            raise ScenarioError(f"Could not update metadata: {result.errors}")

        for warning in result.errors:
            self.add_warning("metadata", warning)
        if isinstance(result.data, dict) and isinstance(result.data.get("scenario"), dict):
            self._session = result.data["scenario"]
        return result.data

    def set_short_name(self, short_name: str) -> None:
        """Set the label workbooks and exports use for this scenario."""
        self.short_name = str(short_name)

    def identifier(self) -> Union[str, int]:
        """The scenario's title, else its short name, else its ID."""
        return self.title or self.short_name or cast(int, self.id)

    # --- Inputs ------------------------------------------------------------------------------

    @property
    def inputs(self) -> Inputs:
        if self._inputs is None:
            data = self._fetch(FetchInputsRunner, "inputs", defaults="original")
            self._inputs = self._collect(Inputs.from_json(data))
        return self._inputs

    def _fetch(self, runner: Any, name: str, **kwargs: Any) -> Any:
        """Run a fetch runner on this scenario, keeping its warnings under the given name."""
        result = runner.run(self.client, self, **kwargs)
        if not result.success:
            raise ScenarioError(f"Could not retrieve {name}: {result.errors}")
        if result.data is None:
            raise ScenarioError(f"No data returned for {name}")
        for warning in result.errors:
            self.add_warning(name, warning)
        return result.data

    def _collect(self, collection: Any) -> Any:
        self._merge_submodel_warnings(collection)
        return collection

    def user_values(self) -> Dict[str, Any]:
        """The values set by the user for inputs."""
        return {inp.key: inp.user for inp in self.inputs if inp.user is not None}

    def set_user_values_from_dataframe(self, dataframe: pd.DataFrame) -> None:
        """Make the inputs match the dataframe's user column, resetting blank ones."""
        series = dataframe["user"]
        if isinstance(series.index, pd.MultiIndex) and "unit" in (series.index.names or []):
            series = series.droplevel("unit")
        self.update_user_values({str(k): v for k, v in series.fillna("reset").to_dict().items()})

    def update_user_values(self, update_inputs: Dict[str, Any], skip_upload: bool = False) -> None:
        """
        Set inputs. Invalid ones are left out of the upload, and the rest are still sent.
        skip_upload changes only the local copy.
        """
        if not skip_upload:
            invalid = self.inputs.is_valid_update(update_inputs)
            valid = {key: value for key, value in update_inputs.items() if key not in invalid}
            if valid:
                self._send(UpdateInputsRunner, "update user values", valid)
        self.inputs.update(update_inputs)

    def _send(self, runner: Any, action: str, *args: Any, **kwargs: Any) -> Any:
        result = runner.run(self.client, self, *args, **kwargs)
        if not result.success:
            raise ScenarioError(f"Could not {action}: {result.errors}")
        return result

    def remove_user_values(self, input_keys: Union[List[str], Set[str]]) -> None:
        """Reset inputs to their defaults."""
        reset_inputs = {key: "reset" for key in input_keys}
        self._send(UpdateInputsRunner, "remove inputs", reset_inputs)
        self.inputs.update(reset_inputs)

    # --- Sortables ---------------------------------------------------------------------------

    @property
    def sortables(self) -> Sortables:
        if self._sortables is None:
            self._sortables = self._collect(Sortables.from_json(self._fetch(FetchSortablesRunner, "sortables")))
        return self._sortables

    def set_sortables_from_dataframe(self, dataframe: pd.DataFrame, skip_upload: bool = False) -> None:
        """Set sortables from a dataframe with sortable names as columns and orders as rows."""
        updates = Sortables._from_dataframe(dataframe).to_updates_dict()
        if updates:
            self.update_sortables(updates, skip_upload=skip_upload)

    def update_sortables(self, update_sortables: Dict[str, List[Any]], skip_upload: bool = False) -> None:
        """
        Set the order of sortables, one call each. Invalid ones are left out of the upload.
        skip_upload changes only the local copy.
        """
        if not skip_upload:
            invalid = self.sortables.is_valid_update(update_sortables)
            for name, order in update_sortables.items():
                if name not in invalid:
                    self._send_sortable(name, order)
        self.sortables.update(update_sortables)

    def _send_sortable(self, name: str, order: List[Any]) -> None:
        if name.startswith("heat_network_"):
            subtype = name.replace("heat_network_", "")
            self._send(UpdateSortablesRunner, f"update sortable '{name}'", "heat_network", order, subtype=subtype)
        else:
            self._send(UpdateSortablesRunner, f"update sortable '{name}'", name, order)

    def remove_sortables(self, sortable_names: Union[List[str], Set[str]]) -> None:
        """Reset sortables to their default orders."""
        for name in sortable_names:
            self._send_sortable(name, [])
        self.sortables.update({name: [] for name in sortable_names})

    # --- Custom curves -----------------------------------------------------------------------

    @property
    def custom_curves(self) -> CustomCurves:
        if self._custom_curves is None:
            data = self._fetch(FetchAllCustomCurveDataRunner, "custom_curves")
            curves = CustomCurves.from_json(cast(List[Dict[str, Any]], data))
            curves._scenario = self
            self._custom_curves = self._collect(curves)
        return self._custom_curves

    def custom_curve_series(self, curve_name: str) -> Optional[pd.Series[Any]]:
        result = self.custom_curves.get_contents(self, curve_name)
        return result if isinstance(result, pd.Series) else None

    def custom_curves_series(self) -> Iterator[Optional[pd.Series[Any]]]:
        """Yield every attached custom curve as a Series."""
        for key in self.custom_curves.attached_keys():
            yield self.custom_curve_series(key)

    def update_custom_curves(self, custom_curves: Any, skip_upload: bool = False) -> None:
        """
        Upload custom curves. Invalid ones are left out with a warning, and the rest are still
        sent. skip_upload changes only the local copy.
        """
        curves = custom_curves.curves
        if not skip_upload:
            curves = self._upload_valid_curves(custom_curves)

        for new_curve in curves:
            existing = self.custom_curves._find(new_curve.key)
            if existing:
                existing.file_path = new_curve.file_path
            else:
                self.custom_curves.curves.append(new_curve)
        self.custom_curves._scenario = self

    def _upload_valid_curves(self, custom_curves: Any) -> List[Any]:
        invalid = custom_curves.validate_for_upload()
        for key, collector in invalid.items():
            self.add_warning("custom_curves", f"Excluded curve '{key}' from upload: {[w.message for w in collector]}")
        if invalid:
            self.auto_show_warnings()

        valid = [curve for curve in custom_curves.curves if curve.key not in invalid]
        if valid:
            self._send(UpdateCustomCurvesRunner, "update custom curves", CustomCurves(curves=valid))
        return valid

    def remove_custom_curves(self, curve_keys: Union[List[str], Set[str]]) -> None:
        """Detach custom curves, and clear their cached files."""
        keys = list(curve_keys)
        if not keys:
            return

        self._send(DeleteCustomCurvesRunner, "remove custom curves", keys)
        for curve in [c for c in self.custom_curves.curves if c.key in keys]:
            curve.remove()
        self.custom_curves.curves = [c for c in self.custom_curves.curves if c.key not in keys]

    # --- Hourly output curves ----------------------------------------------------------------

    @property
    def hourly_output_curves(self) -> HourlyOutputCurves:
        if self._hourly_output_curves is None:
            self._hourly_output_curves = HourlyOutputCurves.create_empty_collection()
        return self._hourly_output_curves

    def get_hourly_curve(self, identifier: str) -> Optional[pd.DataFrame]:
        """
        One hourly output curve, by name or by carrier ('electricity', 'heat', 'hydrogen',
        'methane'), which stands for its primary curve.
        """
        return self.hourly_output_curves.get_curve(identifier, self)

    def get_hourly_curves(self, identifiers: List[str]) -> Dict[str, pd.DataFrame]:
        """Several hourly output curves, by names or carriers, keyed by curve name."""
        return self.hourly_output_curves.get_curves(identifiers, self)

    def all_hourly_output_curves(self) -> Iterator[Optional[pd.DataFrame]]:
        """Yield every hourly output curve."""
        for key in self.hourly_output_curves.attached_keys():
            yield self.get_hourly_curve(key)

    def clear_hourly_curves_cache(self) -> int:
        """Clear the hourly output curves' cached files. Returns how many were removed."""
        return self.hourly_output_curves.clear_cache()

    def clear_custom_curves_cache(self) -> int:
        """Clear the custom curves' cached files. Returns how many were removed."""
        return self.custom_curves.clear_cache()

    def clear_all_curve_caches(self) -> tuple[int, int]:
        """Clear both curve caches. Returns (hourly curves removed, custom curves removed)."""
        return (self.clear_hourly_curves_cache(), self.clear_custom_curves_cache())

    def clear_session_cache(self) -> None:
        """Clear every file cached for this scenario, and the in-memory curve caches."""
        from pyetm.models.hourly_output_curves import _read_csv_cached_impl

        folder = get_settings().path_to_tmp(str(self.id))
        shutil.rmtree(folder, ignore_errors=True)
        folder.mkdir(parents=True, exist_ok=True)

        _read_csv_cached_impl.cache_clear()
        self.hourly_output_curves.warnings.clear()
        self.custom_curves.warnings.clear()

    # --- Annual exports ----------------------------------------------------------------------

    @property
    def annual_exports(self) -> AnnualExports:
        if self._annual_exports is None:
            self._annual_exports = AnnualExports.create_empty_collection()
        return self._annual_exports

    def get_annual_export(self, export_name: str) -> Optional[pd.DataFrame]:
        """One annual export by name."""
        return self.annual_exports.retrieve(self.client, self, export_name)

    def get_annual_exports(
        self, export_names: AnnualExportType | List[AnnualExportType]
    ) -> Dict[str, pd.DataFrame]:
        """Several annual exports by name."""
        names = validate_export_names(cast("str | list[str]", export_names))
        return self.annual_exports.retrieve_multiple(self.client, self, names)

    def set_export_config(self, config: ExportConfig | None) -> None:
        self._export_config = config

    def get_export_config(self) -> ExportConfig | None:
        return self._export_config

    # --- Queries -----------------------------------------------------------------------------

    def add_queries(self, gquery_keys: Union[List[str], Set[str]]) -> None:
        """Request gqueries, run by execute_queries() or results()."""
        if self._queries is None:
            self._queries = Gqueries.from_list(list(gquery_keys))
        else:
            self._queries.add(*gquery_keys)

    def remove_queries(self, *query_keys: str) -> None:
        if self._queries is not None:
            self._queries.remove(*query_keys)

    def clear_queries(self) -> None:
        if self._queries is not None:
            self._queries.clear()

    def execute_queries(self) -> None:
        """Run every requested gquery. Queries run explicitly, once all of them are requested."""
        if self._queries is not None:
            self._queries.execute(self.client, self)
            self._merge_submodel_warnings(self._queries)

    def results(self, columns: Optional[Any] = None) -> pd.DataFrame:
        """The requested gqueries' results, running them first if none has run yet."""
        if self._queries is None or not self.queries_requested():
            return pd.DataFrame()

        if all(value is None for value in self._queries.query_dict.values()):
            self.execute_queries()
        self._report_failed_queries()
        return self._queries.to_dataframe(columns=columns or ["present", "future"])

    def _report_failed_queries(self) -> None:
        queries = cast(Gqueries, self._queries)
        failed = [key for key, value in queries.query_dict.items() if value is None]
        if failed:
            print(f"WARNING: {len(failed)} query(ies) failed:")
            for warning in queries.warnings.get_by_field("results"):
                print(f"  - {warning.message}")

    def queries_requested(self) -> bool:
        return self._queries is not None and len(self._queries.query_keys()) > 0

    # --- Couplings ---------------------------------------------------------------------------

    @property
    def couplings(self) -> Couplings:
        """The scenario's coupling groups."""
        if self._couplings is None:
            self._couplings = self._collect(Couplings.from_json(self._fetch(FetchCouplingsRunner, "couplings")))
        return self._couplings

    def update_couplings(self, coupling_groups: List[str], action: str = "couple", force: bool = False) -> None:
        """Couple or uncouple coupling groups."""
        result = self._send(UpdateCouplingsRunner, "update couplings", coupling_groups, action, force)
        if self._couplings is not None and result.data is not None:
            self._couplings = Couplings.from_json(result.data)
        for warning in result.errors:
            self.add_warning("couplings", warning)

    # --- Warnings and export -----------------------------------------------------------------

    def show_all_warnings(self) -> None:
        """Print every warning on the scenario and on each loaded part of it."""
        print(f"=== Warnings for Scenario {self.id} ===")
        if len(self.warnings) > 0:
            print("\nScenario warnings:")
            self.show_warnings()

        for name, part in self._loaded_parts():
            if len(part.warnings) > 0:
                print(f"\n{name} warnings:")
                part.show_warnings()

    def _loaded_parts(self) -> List[tuple[str, Any]]:
        parts = [
            ("Inputs", self._inputs),
            ("Sortables", self._sortables),
            ("Custom Curves", self._custom_curves),
            ("Hourly Output Curves", self._hourly_output_curves),
            ("Annual Exports", self._annual_exports),
            ("Queries", self._queries),
            ("Couplings", self._couplings),
        ]
        return [(name, part) for name, part in parts if part is not None]

    def to_excel(self, path: PathLike[str] | str, **export_options: Any) -> None:
        """Export this scenario to Excel."""
        from pyetm.models.scenario_packer import ScenarioPacker

        packer = ScenarioPacker()
        packer.add(self)
        packer.to_excel(str(path), **export_options)

    def collect_export_data(self, **export_options: Any) -> "ExportDataCollection":
        """
        DataFrames and dictionaries for any file format (Parquet, CSV, JSON and so on).
        """
        from pyetm.models.scenario_packer import ScenarioPacker

        packer = ScenarioPacker()
        packer.add(self)
        return packer.collect_export_data(**export_options)

    def _to_dataframe(self, **kwargs: Any) -> pd.DataFrame:
        """A single column describing this scenario, for the MAIN sheet."""
        info: Dict[str, Any] = {
            "title": self.title,
            "saved_scenario_id": self.id,
            "session_id": self.scenario_id,
            "private": self.private,
            "area_code": self.area_code,
            "start_year": self.start_year,
            "end_year": self.end_year,
            "keep_compatible": self.keep_compatible,
            "source": self.source,
            "url": self.url,
            "version": self.version,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }
        if self.short_name:
            info["short_name"] = self.short_name
        for key, value in self.metadata.items():
            info.setdefault(key, value)

        return pd.DataFrame.from_dict(info, orient="index", columns=[str(self.id)])

    # --- Applying data after creation --------------------------------------------------------

    def _apply_data(
        self,
        user_values: Optional[Dict[str, Any]],
        custom_curves: Optional[Dict[str, Any]],
        sortables: Optional[Dict[str, Any]],
    ) -> None:
        """Send inputs, curves and sortables to a new scenario. A failure becomes a warning."""
        data = {"user_values": user_values, "custom_curves": custom_curves, "sortables": sortables}
        warnings = apply_data([(self, data)], self.client)
        for warning in warnings:
            self.add_warning("base", warning)
        if warnings:
            self.auto_show_warnings(f"Scenario #{self.id} (title='{self.title}')")


def apply_data(items: List[tuple[Scenario, Dict[str, Any]]], client: BaseClient) -> List[str]:
    """
    Send each scenario's user_values, custom_curves ({key: values}) and sortables ({type: order})
    concurrently. Returns one warning per request that failed.
    """
    requests: List[Dict[str, Any]] = []
    labels: List[str] = []
    for scenario, data in items:
        for request, label in _data_requests(scenario, data):
            requests.append(request)
            labels.append(label)

    results = BaseRunner._make_batch_requests(client, requests) if requests else []
    return [
        f"{label}: {', '.join(result.errors) or 'Unknown error'}"
        for label, result in zip(labels, results)
        if not result.success
    ]


def _data_requests(scenario: Scenario, data: Dict[str, Any]) -> Iterator[tuple[Dict[str, Any], str]]:
    title = scenario.title
    if data.get("user_values"):
        request = UpdateInputsRunner.build_request(scenario, data["user_values"])
        yield request, f"Failed to update user_values for scenario '{title}'"

    curves = data.get("custom_curves") or {}
    for key, request in zip(curves, UpdateCustomCurvesRunner.build_requests(scenario, curves)):
        yield request, f"Failed to upload curve '{key}' for scenario '{title}'"

    for sortable_type, order in (data.get("sortables") or {}).items():
        request = UpdateSortablesRunner.build_request(scenario, sortable_type, order)
        yield request, f"Failed to update sortables for scenario '{title}'"
