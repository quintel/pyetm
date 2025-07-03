from __future__ import annotations

from pyetm.models.base import Base
from .input import Input


class InputCollection(Base):
    inputs: list[Input]

    def __len__(self):
        return len(self.inputs)

    def __iter__(self):
        yield from iter(self.inputs)

    def keys(self):
        return [input.key for input in self.inputs]

    @classmethod
    def from_json(cls, data) -> InputCollection:
        inputs = [Input.from_json(item) for item in data.items()]

        collection = cls.model_validate({"inputs": inputs})

        # Merge any warnings from individual inputs
        for input_obj in inputs:
            if hasattr(input_obj, "warnings") and input_obj.warnings:
                for warning in input_obj.warnings:
                    collection.add_warning(warning)

        return collection
