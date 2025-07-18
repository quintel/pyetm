from __future__ import annotations
import pandas as pd
from datetime import datetime
from typing import Any, Dict, Optional
from urllib.parse import urlparse
from pydantic import Field, PrivateAttr, model_validator
from pyetm.models.inputs import Inputs
from pyetm.models.carrier_curves import CarrierCurves
from pyetm.clients import BaseClient
from pyetm.models.base import Base
from pyetm.models.custom_curves import CustomCurves
from pyetm.models.gqueries import Gqueries
from pyetm.models.sortables import Sortables
from pyetm.services.scenario_runners.fetch_inputs import FetchInputsRunner
from pyetm.services.scenario_runners.fetch_metadata import FetchMetadataRunner
from pyetm.services.scenario_runners.fetch_sortables import FetchSortablesRunner
from pyetm.services.scenario_runners.fetch_custom_curves import (
    FetchAllCustomCurveDataRunner,
)
from pyetm.services.scenario_runners.fetch_carrier_curves import (
    FetchAllCarrierCurvesRunner,
)
from pyetm.services.scenario_runners.update_inputs import UpdateInputsRunner


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
    metadata: Optional[Dict[str, Any]] = None
    start_year: Optional[int] = None
    scaling: Optional[Any] = None
    template: Optional[int] = None
    url: Optional[str] = None

    # private caches for submodels
    _inputs: Optional[Inputs] = PrivateAttr(None)
    _sortables: Optional[Sortables] = PrivateAttr(None)
    _custom_curves: Optional[CustomCurves] = PrivateAttr(default=None)
    _carrier_curves: Optional[CarrierCurves] = PrivateAttr(default=None)
    _queries: Optional[Gqueries] = PrivateAttr(None)

    @classmethod
    def load(cls, scenario_id: int) -> Scenario:
        """
        Fetch metadata for scenario_id, return a Scenario (with warnings if any keys missing).
        """
        temp = type("T", (), {"id": scenario_id})
        result = FetchMetadataRunner.run(BaseClient(), temp)

        if not result.success:
            raise ScenarioError(
                f"Could not load scenario {scenario_id}: {result.errors}"
            )

        # parse into a Scenario
        scenario = cls.model_validate(result.data)
        # attach any metadata‐fetch warnings
        for w in result.errors:
            scenario.add_warning(w)
        return scenario

    def __eq__(self, other: "Scenario"):
        return self.id == other.id

    def __hash__(self):
        return hash((self.id, self.area_code, self.end_year))

    def to_dataframe(self) -> pd.DataFrame:
        return pd.DataFrame.from_dict(
            self.model_dump(include={"end_year", "area_code", "private", "template"}),
            orient="index",
            columns=[self.id],
        )

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
        result = FetchInputsRunner.run(BaseClient(), self)
        if not result.success:
            raise ScenarioError(f"Could not retrieve inputs: {result.errors}")

        coll = Inputs.from_json(result.data)
        # merge runner warnings and any item‐level warnings
        for w in result.errors:
            self.add_warning(w)
        self._merge_submodel_warnings(coll)

        self._inputs = coll
        return coll

    def update_inputs(self, inputs: Dict[str, Any]) -> None:
        """
        Args:
            inputs: Dictionary of input key-value pairs to update
        """
        result = UpdateInputsRunner.run(BaseClient(), self, inputs)

        if not result.success:
            raise ScenarioError(f"Could not update inputs: {result.errors}")

        for w in result.errors:
            self.add_warning(w)

        # Invalidate the cached inputs so they'll be refetched next time
        self._inputs = None

    @property
    def sortables(self) -> Sortables:
        if self._sortables is not None:
            return self._sortables

        result = FetchSortablesRunner.run(BaseClient(), self)
        if not result.success:
            raise ScenarioError(f"Could not retrieve sortables: {result.errors}")

        coll = Sortables.from_json(result.data)
        for w in result.errors:
            self.add_warning(w)
        self._merge_submodel_warnings(coll)

        self._sortables = coll
        return coll

    @property
    def custom_curves(self) -> CustomCurves:
        if self._custom_curves is not None:
            return self._custom_curves

        result = FetchAllCustomCurveDataRunner.run(BaseClient(), self)
        if not result.success:
            raise ScenarioError(f"Could not retrieve custom_curves: {result.errors}")

        coll = CustomCurves.from_json(result.data)
        for w in result.errors:
            self.add_warning(w)
        self._merge_submodel_warnings(coll)

        self._custom_curves = coll
        return coll

    def custom_curve_series(self, curve_name: str) -> pd.Series:
        return self.custom_curves.get_contents(self, curve_name)

    def custom_curves_series(self):
        """Yield all Series"""
        for key in self.custom_curves.attached_keys():
            yield self.custom_curve_series(key)

    @property
    def carrier_curves(self) -> CarrierCurves:
        if self._carrier_curves is not None:
            return self._carrier_curves

        # Create collection with all known curve types
        self._carrier_curves = CarrierCurves.create_empty_collection()
        return self._carrier_curves

    def carrier_curve_series(self, curve_name: str) -> pd.Series:
        return self.carrier_curves.get_contents(self, curve_name)

    def carrier_curves_series(self):
        """Yield all Series"""
        for key in self.carrier_curves.attached_keys():
            yield self.carrier_curve_series(key)

    def add_queries(self, gquery_keys: list[str]):
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

    def results(self, values='future') -> pd.DataFrame:
        """
        Returns the results of the requested queries in a dataframe
        """
        if not self.queries_requested():
            # TODO: Return something nicer, or more useful.
            return pd.DataFrame()

        if not self._queries.is_ready():
            self.execute_queries()

        return self._queries.to_dataframe(values=values)

    def queries_requested(self):
        """
        Returns True if queries have been requested
        """
        if self._queries is None:
            return False

        return len(self._queries.query_keys()) > 0

    ## VALIDATORS

    # NOTE: I left this out, as users cannot set the start year anyways
    # @model_validator(mode="after")
    # def validate_end_year_after_start_year(self):
    #     """Rails: validates :end_year, numericality: { greater_than: start_year }"""
    #     if self.end_year is not None and self.start_year is not None:
    #         if self.end_year <= self.start_year:
    #             raise ValueError(
    #                 f"End year ({self.end_year}) must be greater than start year ({self.start_year})"
    #             )
    #     return self
