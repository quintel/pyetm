import pandas as pd

from __future__ import annotations
from datetime import datetime
from typing import Any, Dict, Optional

from pydantic import Field, PrivateAttr
from pyetm.clients import BaseClient
from pyetm.models.input_collection import InputCollection, CustomCurves
from pyetm.models.sortable_collection import SortableCollection
from pyetm.services.scenario_runners.fetch_inputs import FetchInputsRunner
from pyetm.services.scenario_runners.fetch_metadata import FetchMetadataRunner
from pyetm.services.scenario_runners.fetch_sortables import FetchSortablesRunner
from pyetm.services.custom_curves import fetch_all_curve_data


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
    end_year: Optional[int] = None
    keep_compatible: Optional[bool] = None
    private: Optional[bool] = None
    area_code: Optional[str] = None
    source: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    start_year: Optional[int] = None
    scaling: Optional[Any] = None
    template: Optional[int] = None
    url: Optional[str] = None

    # private caches
    _inputs: Optional[InputCollection] = PrivateAttr(None)
    _sortables: Optional[SortableCollection] = PrivateAttr(None)

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

    @property
    def inputs(self) -> InputCollection:
        # If we already fetched and cached, return it
        if self._inputs is not None:
            return self._inputs

        # Otherwise fetch and cache
        result = FetchInputsRunner.run(BaseClient(), self)
        if not result.success:
            raise ScenarioError(f"Could not retrieve inputs: {result.errors}")

        coll = InputCollection.from_json(result.data)
        # merge runner warnings and any item‐level warnings
        for w in result.errors:
            self.add_warning(w)
        self._merge_submodel_warnings(coll)

        self._inputs = coll
        return coll

    @property
    def sortables(self) -> SortableCollection:
        if self._sortables is not None:
            return self._sortables

        result = FetchSortablesRunner.run(BaseClient(), self)
        if not result.success:
            raise ScenarioError(f"Could not retrieve sortables: {result.errors}")

        coll = SortableCollection.from_json(result.data)
        for w in result.errors:
            self.add_warning(w)
        self._merge_submodel_warnings(coll)

        self._sortables = coll
        return coll

    def user_values(self) -> Dict[str, Any]:
        """Returns only the inputs where a user override is present."""
        return {inp.key: inp.user for inp in self.inputs if inp.user is not None}

   @property
    def custom_curves(self):
        """
        Property to hold the Scenario's InputCollection
        """
        return self._custom_curves

    @custom_curves.setter
    def custom_curves(self, _value):
        """
        TODO: should be removed or reworked, users should not be able to set
        the custom_curves themselves
        """
        return

    @custom_curves.getter
    def custom_curves(self):
        try:
            return self._custom_curves
        except AttributeError:
            result = fetch_all_curve_data(BaseClient(), self)

            if result.success:
                # Make sure to add validation and error collection to the collection as well
                self._custom_curves = CustomCurves.from_json(result.data)
                return self._custom_curves
            else:
                raise ScenarioError(f"Could not retrieve custom_curves: {result.errors}")


    def curve_series(self, curve_name: str) -> pd.Series:
        return self.custom_curves.get_contents(self, curve_name)
