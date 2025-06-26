from datetime import datetime
from typing import Any, Dict, Optional
from pydantic import BaseModel, Field


class Metadata(BaseModel):
    """
    Holds the metadata fields of a Scenario (as returned by the FetchMetadataRunner).
    """

    id: int
    created_at: datetime = Field(..., description="ISO8601 timestamp of creation")
    updated_at: datetime = Field(..., description="ISO8601 timestamp of last update")
    end_year: int
    keep_compatible: bool
    private: bool
    area_code: str
    source: str
    metadata: Optional[Dict[str, Any]] = Field(
        None, description="User‐defined metadata object (up to 64Kb)"
    )
    start_year: Optional[int] = None
    scaling: Optional[Any] = None
    template: Optional[int] = None
    url: Optional[str] = None

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> "Metadata":
        """
        Instantiate from the exact dict returned by FetchMetadataRunner.
        """
        # Pydantic will coerce and validate types (e.g. parse ISO datetimes).
        return cls.model_validate(data)
