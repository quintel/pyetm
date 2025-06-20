from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional, Dict, Any

from pyetm.services.scenario_runners import (
    FetchInputsRunner,
    FetchScenarioRunner,
)
from pyetm.models import InputCollection, BalancedInputCollection
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
        return {input.key: input.user for input in self.inputs if input.user}

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

    def balanced_values(self) -> Dict[str, Any]:
        """Plain dict of key→balanced_value."""
        return {b.key: b.value for b in self.balanced_inputs.inputs}

    @property
    def balanced_inputs(self) -> BalancedInputCollection:
        """
        Lazy-loads balanced inputs by reusing the /scenarios/:id show endpoint.
        """
        try:
            return self._balanced_inputs
        except AttributeError:
            # Fetch the full scenario JSON
            res = FetchScenarioRunner.run(BaseClient(), self)
            if res.success:
                # Extract and map the balanced_values hash:
                data = res.data.get("balanced_values", {})
                self._balanced_inputs = BalancedInputCollection.from_json(data)
                return self._balanced_inputs
            else:
                raise ScenarioError(f"Could not retrieve balanced values: {res.errors}")

    @balanced_inputs.setter
    def balanced_inputs(self, val: BalancedInputCollection):
        self._balanced_inputs = val


class ScenarioError(BaseException):
    """Base scenario error"""
