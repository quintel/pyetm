from pydantic import BaseModel, Field
from pyetm.clients import BaseClient


class Scenario(BaseModel):
    """
    Pydantic model for an ETM Scenario, matching the DB schema,
    but with only `id` required so it can be used for API runners.
    """

    id: int = Field(..., description="Unique scenario identifier")

    def user_values(self):
        """
        Returns the values set by the user
        """
        return {
            input.key: input.user for input in self.inputs if input.user is not None
        }

    @property
    def inputs(self):
        """
        Property to hold the Scenario's InputCollection
        """
        return self._inputs

    @inputs.setter
    def inputs(self, value):
        """
        TODO: should be removed or reworked, users should not be able to set
        the inputs themselves
        """
        self._inputs = value

    @inputs.getter
    def inputs(self):
        from pyetm.services.scenario_runners import FetchInputsRunner
        from pyetm.models.input_collection import InputCollection

        try:
            return self._inputs
        except AttributeError:
            result = FetchInputsRunner.run(BaseClient(), self)

            if result.success:
                # TODO Make sure to add validation and error collection to the collection as well
                self._inputs = InputCollection.from_json(result.data)
                return self._inputs
            else:
                raise ScenarioError(f"Could not retrieve inputs: {result.errors}")

    @property
    def sortables(self):
        """
        Property to hold the Scenario's SortableCollection
        """
        return self._sortables

    @sortables.setter
    def sortables(self, value):
        """
        Allow explicit setting if needed (e.g. in tests)
        """
        self._sortables = value

    @sortables.getter
    def sortables(self):
        from pyetm.services.scenario_runners import FetchSortablesRunner
        from pyetm.models.sortable_collection import SortableCollection

        try:
            return self._sortables
        except AttributeError:
            result = FetchSortablesRunner.run(BaseClient(), self)
            if result.success:
                self._sortables = SortableCollection.from_json(result.data)
                return self._sortables
            else:
                raise ScenarioError(f"Could not retrieve sortables: {result.errors}")

    # --- VALIDATION ---

    # We should have an error object always there to collect that we can insert stuff into!?
    # And a 'valid' method?
    # Should it be a model?


class ScenarioError(BaseException):
    """Base scenario error"""
