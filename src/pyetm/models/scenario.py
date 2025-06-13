from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional

class Scenario(BaseModel):
    """
    Pydantic model for an ETM Scenario, matching the DB schema,
    but with only `id` required so it can be used for API runners.
    # TODO: investigate filling this out properly with more validation etc
    """
    id: int = Field(..., description="Unique scenario identifier")

    #TODO: These ones are just placeholders effectively for now
    created_at: Optional[datetime] = Field(
        None,
        description="Timestamp when the scenario was created"
    )
    updated_at: Optional[datetime] = Field(
        None,
        description="Timestamp when the scenario was last updated"
    )
    end_year: Optional[int] = Field(
        2040,
        description="Year in which the scenario ends"
    )
    keep_compatible: Optional[bool] = Field(
        False,
        description="These scenarios will be migrated with the changes in the model over time"
    )
    private: Optional[bool] = Field(
        False,
        description="Publicity of scenario"
    )
    preset_scenario_id: Optional[int] = Field(
        None,
        description="ID of the preset scenario"
    )
    area_code: Optional[str] = Field(
        None,
        description="Geographical area code of the scenario"
    )
    source: Optional[str] = Field(
        None,
        description="Where was the scenario made"
    )
    balanced_values: Optional[str] = Field(
        None,
        description="Balanced values computed by the API"
    )
    metadata: Optional[str] = Field(
        None,
        description="Scenario metadata"
    )
    active_couplings: Optional[str] = Field(
        None,
        description="Active coupling groups"
    )

    def user_values(self):
        '''
        Returns the values set by the user
        '''
        return { input.key: input.user for input in self.inputs if input.user }

    @property
    def inputs(self):
        return self._inputs

    @inputs.setter
    def inputs(self, value):
        self._inputs = value

    @inputs.getter
    def inputs(self):
        try:
            return self._inputs
        except AttributeError:
            return {}
