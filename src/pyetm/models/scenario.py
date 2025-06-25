from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional

from pyetm.services.scenario_runners import FetchInputsRunner
from pyetm.models import InputCollection
from pyetm.clients import BaseClient


class Scenario(BaseModel):
    """
    Pydantic model for an ETM Scenario, matching the DB schema,
    but with only `id` required so it can be used for API runners.
    # TODO: investigate filling this out properly with more validation etc
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
        try:
            return self._inputs
        except AttributeError:
            result = FetchInputsRunner.run(BaseClient(), self)

            if result.success:
                # Make sure to add validation and error collection to the collection as well
                self._inputs = InputCollection.from_json(result.data)
                return self._inputs
            else:
                raise ScenarioError(f"Could not retrieve inputs: {result.errors}")

    # --- VALIDATION ---

    # We should have an error object always there to collect that we can insert stuff into!?
    # And a 'valid' method?
    # Should it be a model?


class ScenarioError(BaseException):
    """Base scenario error"""
