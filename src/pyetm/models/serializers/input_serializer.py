from typing import Union, Type
import pandas as pd
from pydantic import BaseModel

from .base_serializer import BaseDataSerializer, SerializationMode


class InputsSerializer(BaseDataSerializer):
    """Serializer for Inputs collection"""

    def serialize(
        self, model: BaseModel, mode: SerializationMode
    ) -> Union[pd.DataFrame, dict]:
        if mode == SerializationMode.DATAFRAME:
            columns = ["unit", "value", "default"]
            df = pd.DataFrame.from_dict(
                {
                    input_obj.key: [input_obj.unit, input_obj.user, input_obj.default]
                    for input_obj in model.inputs
                },
                orient="index",
                columns=columns,
            )
            df.index.name = "input"  # For the packer
            return df

        elif mode == SerializationMode.JSON:
            return {
                "inputs": [input_obj.model_dump() for input_obj in model.inputs],
                "count": len(model.inputs),
            }

        elif mode == SerializationMode.DICT_EXPORT:
            return {input_obj.key: input_obj.model_dump() for input_obj in model.inputs}

        else:
            raise ValueError(f"Unsupported mode {mode} for Inputs")

    def deserialize(
        self,
        data: Union[pd.DataFrame, dict],
        model_class: Type[BaseModel],
        mode: SerializationMode,
    ) -> BaseModel:
        # TODO implement
        pass
