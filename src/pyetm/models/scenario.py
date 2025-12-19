from __future__ import annotations
import pandas as pd
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Union
from urllib.parse import urlparse
from pydantic import Field, PrivateAttr
from os import PathLike
from pyetm.models.couplings import Couplings
from pyetm.models.inputs import Inputs
from pyetm.models.output_curves import OutputCurves
from pyetm.clients import BaseClient
from pyetm.models.base import Base
from pyetm.models.custom_curves import CustomCurves
from pyetm.models.gqueries import Gqueries
from pyetm.models.sortables import Sortables
from pyetm.models.export_config import ExportConfig
from pyetm.services.scenario_runners.fetch_inputs import FetchInputsRunner
from pyetm.services.scenario_runners.fetch_metadata import FetchMetadataRunner
from pyetm.services.scenario_runners.fetch_sortables import FetchSortablesRunner
from pyetm.services.scenario_runners.fetch_custom_curves import (
    FetchAllCustomCurveDataRunner,
)
from pyetm.services.scenario_runners.update_inputs import UpdateInputsRunner
from pyetm.services.scenario_runners.update_sortables import UpdateSortablesRunner
from pyetm.services.scenario_runners.create_scenario import CreateScenarioRunner
from pyetm.services.scenario_runners.update_metadata import UpdateMetadataRunner
from pyetm.services.scenario_runners.update_custom_curves import (
    UpdateCustomCurvesRunner,
)
from pyetm.services.scenario_runners.fetch_couplings import FetchCouplingsRunner
from pyetm.services.scenario_runners.update_couplings import UpdateCouplingsRunner
from pyetm.services.scenario_runners.copy_scenario import CopyScenarioRunner
from pyetm.services.scenario_runners.break_preset_link import BreakPresetLinkRunner


class ScenarioError(Exception):
    """Base scenario error"""


# TODO: Tidy up the datetime created_at and updated-at fields - they come through like:  'created_at': datetime.datetime(2025, 6, 25, 14, 18, 6, tzinfo=TzInfo(UTC))
class Scenario(Base):
    """
    Pydantic model for an ETM Scenario, matching the DB schema,
    but with only `id` required so it can be used for API runners.
    """

    id: int = Field(..., description="Unique scenario identifier")
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    end_year: int = Field(..., description="End year")
    keep_compatible: Optional[bool] = None
    private: Optional[bool] = None
    area_code: str = Field(..., description="Area code")
    source: Optional[str] = None
    title: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    short_name: Optional[str] = None
    start_year: Optional[int] = None
    scaling: Optional[Any] = None
    template: Optional[int] = Field(
        None, description="ID of the preset scenario this was copied from"
    )
    url: Optional[str] = None

    # private caches for submodels
    _inputs: Optional[Inputs] = PrivateAttr(None)
    _sortables: Optional[Sortables] = PrivateAttr(None)
    _custom_curves: Optional[CustomCurves] = PrivateAttr(default=None)
    _output_curves: Optional[OutputCurves] = PrivateAttr(default=None)
    _queries: Optional[Gqueries] = PrivateAttr(None)
    _export_config: Optional[ExportConfig] = PrivateAttr(default=None)
    _couplings: Optional[Couplings] = PrivateAttr(default=None)

    @classmethod
    def new(cls, area_code: str, end_year: int, **kwargs) -> "Scenario":
        """
        Create a new scenario with the specified parameters.

        Returns:
            A new Scenario instance
        """
        scenario_data = {"area_code": area_code, "end_year": end_year, **kwargs}
        result = CreateScenarioRunner.run(BaseClient(), scenario_data)

        if not result.success:
            raise ScenarioError(f"Could not create scenario: {result.errors}")

        scenario = cls.model_validate(result.data)
        for warning in result.errors:
            scenario.add_warning("base", warning)

        for field, value in scenario_data.items():
            if hasattr(scenario, field) and field not in result.data:
                setattr(scenario, field, value)

        return scenario

    @classmethod
    def load(cls, scenario_id: int) -> Scenario:
        """
        Fetch metadata for scenario_id, return a Scenario (with warnings if any keys missing).
        """
        template = type("T", (), {"id": scenario_id})
        result = FetchMetadataRunner.run(BaseClient(), template)

        if not result.success:
            raise ScenarioError(
                f"Could not load scenario {scenario_id}: {result.errors}"
            )

        # parse into a Scenario
        scenario = cls.model_validate(result.data)
        for w in result.errors:
            scenario.add_warning("metadata", w)
        return scenario

    def copy_with_preset(self, **overrides) -> "Scenario":
        """
        Create a copy of this scenario using ETEngine's copy utility.
        The copied scenario will have its template field set to this scenario's ID.
        """
        copy_roles = overrides.pop("copy_roles", False)
        if copy_roles:
            overrides["set_preset_roles"] = copy_roles

        # Call CopyScenarioRunner
        result = CopyScenarioRunner.run(BaseClient(), self.id, overrides=overrides)

        if not result.success:
            raise ScenarioError(f"Failed to copy scenario: {result.errors}")

        # Create and return new scenario
        scenario = Scenario.model_validate(result.data)
        for warning in result.errors:
            scenario.add_warning("base", warning)

        return scenario

    def copy(self, **overrides) -> "Scenario":
        """
        Create a copy with no template link to the original scenario.
        """
        # Create the copy
        new_scenario = self.copy_with_preset(**overrides)

        # Break the template link
        result = BreakPresetLinkRunner.run(BaseClient(), new_scenario)

        if not result.success:
            raise ScenarioError(
                f"Copied scenario but failed to break template link: {result.errors}"
            )

        if result.data and "scenario" in result.data:
            scenario_data = result.data["scenario"]
            for field, value in scenario_data.items():
                if hasattr(new_scenario, field):
                    setattr(new_scenario, field, value)

        return new_scenario

    @classmethod
    def from_excel(cls, xlsx_path: PathLike | str) -> "Scenario":
        """
        Convenience method to load a single scenario from Excel.
        """
        from pyetm.utils.scenario_excel_service import ScenarioExcelService

        scenarios = ScenarioExcelService.import_from_excel(xlsx_path)
        if len(scenarios) != 1:
            raise ScenarioError(
                f"Expected one scenario, found {len(scenarios)}. "
                "Use Scenarios.from_excel() for multi-scenario workbooks."
            )
        return scenarios[0]

    def to_excel(self, path: PathLike | str, **export_options) -> None:
        """
        Convenience method to export this scenario to Excel.
        """
        from pyetm.utils.scenario_excel_service import ScenarioExcelService

        ScenarioExcelService.export_to_excel([self], path, **export_options)

    def save(
        self, client: Optional[BaseClient] = None, title: Optional[str] = None, **kwargs
    ):
        """
        Save this scenario to MyETM as a SavedScenario.

        Returns:
            SavedScenario instance
        """
        from pyetm.models.saved_scenario import SavedScenario

        client = client or BaseClient()

        save_title = title or self.title
        if not save_title:
            raise ScenarioError(
                "Title is required to save scenario. Provide title parameter or set scenario.title"
            )

        params = {
            "scenario_id": self.id,
            "title": save_title,
            **kwargs,
        }

        if self.private is not None:
            params.setdefault("private", self.private)

        return SavedScenario.create(params, client=client)

    def update_metadata(self, **kwargs) -> Dict[str, Any]:
        """
        Update metadata for this scenario.
        """
        result = UpdateMetadataRunner.run(BaseClient(), self, kwargs)

        if not result.success:
            raise ScenarioError(f"Could not update metadata: {result.errors}")

        # Add any warnings from the update
        for w in result.errors:
            self.add_warning("metadata", w)

        # Update the current scenario object with the server response
        if result.data and "scenario" in result.data:
            scenario_data = result.data["scenario"]
            for field, value in scenario_data.items():
                if hasattr(self, field):
                    setattr(self, field, value)

            for field, value in kwargs.items():
                if hasattr(self, field) and field not in scenario_data:
                    setattr(self, field, value)

        return result.data

    def __eq__(self, other: "Scenario"):
        return self.id == other.id

    def __hash__(self):
        return hash((self.id, self.area_code, self.end_year))

    def _to_dataframe(self, **kwargs) -> pd.DataFrame:
        """
        Return a single-column DataFrame describing this scenario
        - Column name is the scenario identifier (short_name/title/id)
        """
        info: Dict[str, Any] = {
            "title": self.title,
            "scenario_id": self.id,
            "template": self.template,
            "area_code": self.area_code,
            "start_year": self.start_year,
            "end_year": self.end_year,
            "keep_compatible": self.keep_compatible,
            "private": self.private,
            "source": self.source,
            "url": self.url,
            "version": self.version,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

        # Description from metadata (if present)
        meta = self.metadata if isinstance(self.metadata, dict) else None
        if meta is not None:
            desc = meta.get("description")
            if desc is not None:
                info["description"] = desc

        # Flatten remaining metadata keys
        if meta is not None:
            for k, v in meta.items():
                if k == "description":
                    continue
                if k not in info:
                    info[k] = v

        col_name = self.identifier() if self.identifier() is not None else self.id
        return pd.DataFrame.from_dict(info, orient="index", columns=[col_name])

    def set_short_name(self, short_name: str):
        """Set the short_name attribute and persist in metadata."""
        self.short_name = str(short_name)
        if self.metadata is not None:
            self.metadata["short_name"] = str(short_name)
        else:
            self.metadata = {"short_name": str(short_name)}

    def identifier(self):
        if self.short_name:
            return self.short_name
        if self.title:
            return self.title
        return self.id

    def user_values(self) -> Dict[str, Any]:
        """
        Returns the values set by the user for inputs
        """
        return {inp.key: inp.user for inp in self.inputs if inp.user is not None}

    @property
    def version(self) -> str:
        """
        Returns the version of the ETM the scenario was made in
        """
        if not self.url:
            return ""

        url_parts = urlparse(self.url).netloc.split(".engine.")
        if len(url_parts) == 1:
            return "latest"

        return url_parts[0]

    @property
    def inputs(self) -> Inputs:
        # If we already fetched and cached, return it
        if self._inputs is not None:
            return self._inputs

        # Otherwise fetch and cache
        result = FetchInputsRunner.run(BaseClient(), self, defaults="original")
        if not result.success:
            raise ScenarioError(f"Could not retrieve inputs: {result.errors}")

        coll = Inputs.from_json(result.data)
        # merge runner warnings and any item‐level warnings
        for w in result.errors:
            self.add_warning("inputs", w)
        # Merge submodel warnings with a simple, clean prefix
        self._merge_submodel_warnings(coll)

        self._inputs = coll
        return coll

    def set_user_values_from_dataframe(self, dataframe: pd.DataFrame) -> None:
        """
        Extract df to dict, set None/NaN sliders to reset, and call update_inputs.
        This ensures the dataframe exactly represents the inputs.
        """
        series = dataframe["user"]
        # If MultiIndex with 'unit', drop it
        if isinstance(series.index, pd.MultiIndex) and "unit" in (
            series.index.names or []
        ):
            series = series.droplevel("unit")
        self.update_user_values(series.fillna("reset").to_dict())

    def update_user_values(self, update_inputs: Dict[str, Any], skip_upload: bool = False) -> None:
        """
        Args:
            update_inputs: Dictionary of input key-value pairs to update
            skip_upload: If True, skip API call and validation, only update local cache
        """
        if not skip_upload:
            # Update them in the Inputs object, and check validation
            validity_errors = self.inputs.is_valid_update(update_inputs)
            self._handle_validity_errors(validity_errors, "user values")

            result = UpdateInputsRunner.run(BaseClient(), self, update_inputs)

            if not result.success:
                raise ScenarioError(f"Could not update user values: {result.errors}")

        self.inputs.update(update_inputs)

    def remove_user_values(self, input_keys: Union[List[str], Set[str]]) -> None:
        """
        Remove user values for specified inputs, resetting them to default values.

        Args:
            input_keys: List or set of input keys to reset to default values
        """
        reset_inputs = {key: "reset" for key in input_keys}
        result = UpdateInputsRunner.run(BaseClient(), self, reset_inputs)

        if not result.success:
            raise ScenarioError(f"Could not remove inputs: {result.errors}")

        # Update them in the Inputs object
        self.inputs.update(reset_inputs)

    @property
    def sortables(self) -> Sortables:
        if self._sortables is not None:
            return self._sortables

        result = FetchSortablesRunner.run(BaseClient(), self)
        if not result.success:
            raise ScenarioError(f"Could not retrieve sortables: {result.errors}")

        coll = Sortables.from_json(result.data)
        for w in result.errors:
            self.add_warning("sortables", w)
        # Merge submodel warnings with a simple, clean prefix
        self._merge_submodel_warnings(coll)

        self._sortables = coll
        return coll

    def set_sortables_from_dataframe(self, dataframe: pd.DataFrame, skip_upload: bool = False) -> None:
        """
        Extract sortables from dataframe and update them.
        The dataframe should have sortable names as columns and orders as rows.

        Args:
            dataframe: DataFrame with sortable names as columns and order values as rows
            skip_upload: If True, skip API call and validation, only update local cache
        """
        coll = Sortables._from_dataframe(dataframe)
        updates = coll.to_updates_dict()
        if updates:
            self.update_sortables(updates, skip_upload=skip_upload)

    def update_sortables(self, update_sortables: Dict[str, List[Any]], skip_upload: bool = False) -> None:
        """
        Update the order of specified sortables.

        Args:
            update_sortables: Dictionary mapping sortable names to their new orders
            skip_upload: If True, skip API calls and validation, only update local cache
        """
        if not skip_upload:
            # Validate the updates first
            validity_errors = self.sortables.is_valid_update(update_sortables)
            self._handle_validity_errors(validity_errors, "sortables")

            # Make individual API calls for each sortable as there is no bulk endpoint
            for name, order in update_sortables.items():
                if name.startswith("heat_network_"):
                    subtype = name.replace("heat_network_", "")
                    result = UpdateSortablesRunner.run(
                        BaseClient(), self, "heat_network", order, subtype=subtype
                    )
                else:
                    result = UpdateSortablesRunner.run(BaseClient(), self, name, order)

                if not result.success:
                    raise ScenarioError(
                        f"Could not update sortable '{name}': {result.errors}"
                    )

        self.sortables.update(update_sortables)

    def remove_sortables(self, sortable_names: Union[List[str], Set[str]]) -> None:
        """
        Reset specified sortables to their default/empty orders.

        Args:
            sortable_names: List or set of sortable names to reset
        """
        # Make individual API calls to reset each sortable
        for name in sortable_names:
            if name.startswith("heat_network_"):
                # Handle heat_network with subtype
                subtype = name.replace("heat_network_", "")
                result = UpdateSortablesRunner.run(
                    BaseClient(), self, "heat_network", [], subtype=subtype
                )
            else:
                result = UpdateSortablesRunner.run(BaseClient(), self, name, [])

            if not result.success:
                raise ScenarioError(
                    f"Could not remove sortable '{name}': {result.errors}"
                )

        reset_sortables = {name: [] for name in sortable_names}
        self.sortables.update(reset_sortables)

    @property
    def custom_curves(self) -> CustomCurves:
        if self._custom_curves is not None:
            return self._custom_curves

        result = FetchAllCustomCurveDataRunner.run(BaseClient(), self)
        if not result.success:
            raise ScenarioError(f"Could not retrieve custom_curves: {result.errors}")

        coll = CustomCurves.from_json(result.data)
        try:
            coll._scenario = self
        except Exception:
            pass
        for w in result.errors:
            self.add_warning("custom_curves", w)
        # Merge submodel warnings with a simple, clean prefix
        self._merge_submodel_warnings(coll)

        self._custom_curves = coll
        return coll

    def custom_curve_series(self, curve_name: str) -> pd.Series:
        return self.custom_curves.get_contents(self, curve_name)

    def custom_curves_series(self):
        """Yield all Series"""
        for key in self.custom_curves.attached_keys():
            yield self.custom_curve_series(key)

    def update_custom_curves(self, custom_curves, skip_upload: bool = False) -> None:
        """
        Upload/update custom curves for this scenario.

        Args:
            custom_curves: CustomCurves object containing curves to upload
            skip_upload: If True, skip API call and validation, only update local cache
        """
        if not skip_upload:
            # Validate curves before uploading
            validity_errors = custom_curves.validate_for_upload()
            self._handle_validity_errors(validity_errors, "custom curves")

            # Upload curves
            result = UpdateCustomCurvesRunner.run(BaseClient(), self, custom_curves)
            if not result.success:
                raise ScenarioError(f"Could not update custom curves: {result.errors}")

        # TODO: this should be done in custom curves
        # Update the scenario's custom curves object
        for new_curve in custom_curves.curves:
            existing_curve = self.custom_curves._find(new_curve.key)
            if existing_curve:
                existing_curve.file_path = new_curve.file_path
            else:
                self.custom_curves.curves.append(new_curve)
        try:
            self.custom_curves._scenario = self
        except Exception:
            pass

    @property
    def output_curves(self) -> OutputCurves:
        if self._output_curves is not None:
            return self._output_curves

        # Create collection with all known curve types
        self._output_curves = OutputCurves.create_empty_collection()
        return self._output_curves

    def output_curve(self, curve_name: str) -> pd.DataFrame:
        return self.output_curves.get_contents(self, curve_name)

    def all_output_curves(self):
        for key in self.output_curves.attached_keys():
            yield self.output_curve(key)

    def get_output_curves(self, carrier_type: str) -> dict[str, pd.DataFrame]:
        return self.output_curves.get_curves_by_carrier_type(self, carrier_type)

    def set_export_config(self, config: ExportConfig | None) -> None:
        self._export_config = config

    def get_export_config(self) -> ExportConfig | None:
        return self._export_config

    def add_queries(self, gquery_keys: list[str] | set[str]):
        if self._queries is None:
            self._queries = Gqueries.from_list(gquery_keys)
        else:
            self._queries.add(*gquery_keys)

    def execute_queries(self):
        """
        Queries are executed explicitly, as we need to know when the user is
        ready collecting all of them
        """
        self._queries.execute(BaseClient(), self)
        self._merge_submodel_warnings(self._queries)

    def results(self, columns=None) -> pd.DataFrame:
        """
        Returns the results of the requested queries in a dataframe
        """
        if columns is None:
            columns = ["present", "future"]

        if not self.queries_requested():
            return pd.DataFrame()

        if not self._queries.is_ready():
            self.execute_queries()

        return self._queries.to_dataframe(columns=columns)

    def queries_requested(self):
        """
        Returns True if queries have been requested
        """
        if self._queries is None:
            return False

        return len(self._queries.query_keys()) > 0

    def show_all_warnings(self) -> None:
        """
        Display all warnings from the scenario and its submodels in a organized way.
        """
        print(f"=== Warnings for Scenario {self.id} ===")

        # Show scenario-level warnings
        if len(self.warnings) > 0:
            print("\nScenario warnings:")
            self.show_warnings()

        # Show submodel warnings if they exist and are loaded
        submodels = [
            ("Inputs", self._inputs),
            ("Sortables", self._sortables),
            ("Custom Curves", self._custom_curves),
            ("Output Curves", self._output_curves),
            ("Queries", self._queries),
            ("Couplings", self._couplings),
        ]

        for name, submodel in submodels:
            if submodel is not None and len(submodel.warnings) > 0:
                print(f"\n{name} warnings:")
                submodel.show_warnings()

    def _handle_validity_errors(
        self, validity_errors: Dict[str, Any], context: str
    ) -> None:
        """
        Helper method to format and raise ScenarioError for validity errors.
        """
        if not validity_errors:
            return

        error_summary = []
        for key, warning_collector in validity_errors.items():
            warnings_list = [w.message for w in warning_collector]
            error_summary.append(f"{key}: {warnings_list}")

        raise ScenarioError(f"Could not update {context}: {error_summary}")

    @property
    def couplings(self) -> Couplings:
        """Get coupling groups for this scenario"""
        if self._couplings is not None:
            return self._couplings

        result = FetchCouplingsRunner.run(BaseClient(), self)
        if not result.success:
            raise ScenarioError(f"Could not retrieve couplings: {result.errors}")

        coll = Couplings.from_json(result.data)
        for w in result.errors:
            self.add_warning("couplings", w)
        self._merge_submodel_warnings(coll)

        self._couplings = coll
        return coll

    def update_couplings(
        self, coupling_groups: List[str], action: str = "couple", force: bool = False
    ) -> None:

        result = UpdateCouplingsRunner.run(
            BaseClient(), self, coupling_groups, action, force
        )

        if not result.success:
            raise ScenarioError(f"Could not update couplings: {result.errors}")

        # Update the cached couplings with the response data
        if self._couplings is not None:
            updated_couplings = Couplings.from_json(result.data)
            self._couplings = updated_couplings

        for w in result.errors:
            self.add_warning("couplings", w)
