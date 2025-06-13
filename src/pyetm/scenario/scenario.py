from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional

class Scenario(BaseModel):
    """
    Base model for scenario.
    """
    id: int
    created_at: datetime = Field(..., description="Timestamp when the scenario was created")
    updated_at: datetime = Field(..., description="Timestamp when the scenario was last updated")
    end_year: int = Field(2040, description="Year in which the scenario ends")
    keep_compatible: bool = Field(False, description="Maintain compatibility with parent scenario")
    private: bool = Field(False, description="Whether the scenario is private")
    preset_scenario_id: Optional[int] = Field(
        None, description="ID of the preset scenario, if any"
    )
    area_code: Optional[str] = Field(
        None, description="Geographical area code of the scenario"
    )
    source: Optional[str] = Field(
        None, description="Origin or source identifier for the scenario"
    )

    #TODO: Is it useful to describe which columns are binary and which aren't? Everything is JSON anyway... Maybe more helpful to describe what it 'should' be
    user_values: Optional[str] = Field(
        None, description="Base64 blob of user-changed input values"
    )
    balanced_values: Optional[str] = Field(
        None, description="Base64 blob of balanced values computed by the API"
    )
    metadata: Optional[str] = Field(
        None, description="Base64 blob of scenario metadata"
    )
    active_couplings: Optional[str] = Field(
        None, description="Base64 blob of active coupling groups"
    )
