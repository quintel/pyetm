from __future__ import annotations
from typing import List, Set
from pyetm.models.base import Base

import pandas as pd


class Couplings(Base):
    """
    Represents active and inactive couplings of a scenario.
    """

    active_couplings: List[str] = []
    inactive_couplings: List[str] = []

    def __init__(self, **data):
        super().__init__(**data)

    def empty(self) -> bool:
        return len(self.all_groups()) == 0

    def active_groups(self) -> List[str]:
        """Get active coupling groups"""
        return self.active_couplings

    def inactive_groups(self) -> List[str]:
        """Get inactive coupling groups"""
        return self.inactive_couplings

    def all_groups(self) -> Set[str]:
        """Get all coupling groups (active and inactive)"""
        return set(self.active_couplings + self.inactive_couplings)

    def to_series(self, name="") -> pd.Series:
        return pd.Series(
            [self.active_couplings, self.inactive_couplings],
            index=["active_couplings", "inactive_couplings"],
            name=name
        )

    @classmethod
    def from_json(cls, data: dict) -> Couplings:
        """
        Create Couplings from JSON data.

        Expected format:
        {
            "active_couplings": ["external_group1"],
            "inactive_couplings": ["external_group2"]
        }
        """
        return cls.model_validate(
            {
                "active_couplings": data.get("active_couplings", []),
                "inactive_couplings": data.get("inactive_couplings", []),
            }
        )
