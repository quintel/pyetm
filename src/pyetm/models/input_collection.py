from pydantic import BaseModel
from .input import Input


class InputCollection(BaseModel):
    inputs: list["Input"]

    def __len__(self):
        return len(self.inputs)

    def __iter__(self):
        yield from iter(self.inputs)

    def keys(self):
        return [input.key for input in self.inputs]

    @classmethod
    def from_json(cls, data):
        return cls(inputs=[Input.from_json(item) for item in data.items()])
