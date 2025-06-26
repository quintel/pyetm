from pydantic import BaseModel, Field
from datetime import datetime
from typing import Any, Dict, Optional
from pyetm.clients import BaseClient
from pyetm.models.input_collection import InputCollection
from pyetm.models.sortable_collection import SortableCollection
from pyetm.services.scenario_runners.fetch_inputs import FetchInputsRunner
from pyetm.services.scenario_runners.fetch_metadata import FetchMetadataRunner
from pyetm.services.scenario_runners.fetch_sortables import FetchSortablesRunner


class ScenarioError(BaseException):
    """Base scenario error"""


# TODO: Tidy up the datetime created_at and updated-at fields - they come through like:  'created_at': datetime.datetime(2025, 6, 25, 14, 18, 6, tzinfo=TzInfo(UTC))
class Scenario(BaseModel):
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

    # internal caches for inputs and sortables - these act as the properties
    _inputs: Optional[InputCollection] = None
    _sortables: Optional[SortableCollection] = None

    @classmethod
    def load(cls, scenario_id: int) -> "Scenario":
        """
        Factory method: fetch metadata for scenario_id and return a fully populated Scenario.
        """
        # Use a simple object with only id to call the runner
        temp = type("T", (), {"id": scenario_id})
        result = FetchMetadataRunner.run(BaseClient(), temp)
        if not result.success:
            raise ScenarioError(
                f"Could not load scenario {scenario_id}: {result.errors}"
            )
        return cls.model_validate(result.data)

    def user_values(self) -> Dict[str, Any]:
        """
        Returns the values set by the user for inputs
        """
        return {inp.key: inp.user for inp in self.inputs if inp.user is not None}

    @property
    def inputs(self) -> InputCollection:
        # If we already fetched and cached, return it
        if self._inputs is not None:
            return self._inputs

        # Otherwise fetch and cache
        result = FetchInputsRunner.run(BaseClient(), self)
        if not result.success:
            raise ScenarioError(f"Could not retrieve inputs: {result.errors}")
        self._inputs = InputCollection.from_json(result.data)
        return self._inputs

    @inputs.setter
    def inputs(self, value: InputCollection) -> None:
        """
        TODO: should be removed or reworked, users should not be able to set
        the inputs themselves
        """
        self._inputs = value

    @property
    def sortables(self) -> SortableCollection:
        """
        Lazy-loaded SortableCollection for this scenario
        """
        # if we’ve already fetched once, it’ll be in the cache
        if self._sortables is not None:
            return self._sortables

        # otherwise call the runner and cache
        result = FetchSortablesRunner.run(BaseClient(), self)
        if not result.success:
            raise ScenarioError(f"Could not retrieve sortables: {result.errors}")
        self._sortables = SortableCollection.from_json(result.data)
        return self._sortables

    @sortables.setter
    def sortables(self, value: SortableCollection) -> None:
        """
        Allow explicit setting if needed (e.g. in tests)
        """
        self._sortables = value

    # --- VALIDATION ---

    # We should have an error object always there to collect that we can insert stuff into!?
    # And a 'valid' method?
    # Should it be a model?
