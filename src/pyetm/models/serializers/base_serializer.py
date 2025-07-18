from abc import ABC, abstractmethod
from typing import Any, Dict, Type, TypeVar, Optional, Union, ClassVar
import pandas as pd
from pydantic import BaseModel, field_serializer, model_serializer
from pathlib import Path
from enum import Enum


# Type variables
T = TypeVar("T", pd.DataFrame, pd.Series, dict)
M = TypeVar("M", bound=BaseModel)


class SerializationMode(Enum):
    """Different serialization modes for different use cases"""

    JSON = "json"  # For API responses - clean JSON
    DATAFRAME = "dataframe"  # For Excel export - DataFrame format
    SERIES = "series"  # For time series data - Series format
    DICT_EXPORT = "dict_export"  # For dictionary-based exports


class BaseDataSerializer(ABC):
    """Abstract base for data serializers"""

    @abstractmethod
    def serialize(
        self, model: BaseModel, mode: SerializationMode
    ) -> Union[pd.DataFrame, pd.Series, dict]:
        """Serialize model according to the specified mode"""
        pass

    @abstractmethod
    def deserialize(
        self,
        data: Union[pd.DataFrame, pd.Series, dict],
        model_class: Type[BaseModel],
        mode: SerializationMode,
    ) -> BaseModel:
        """Deserialize data back to model"""
        pass


class SerializerMixin:
    """Mixin that adds serialization capabilities to models"""

    _serializer: ClassVar[Optional[BaseDataSerializer]] = None

    def serialize_to_dataframe(self) -> pd.DataFrame:
        """Serialize to DataFrame for Excel export"""
        if self._serializer:
            return self._serializer.serialize(self, SerializationMode.DATAFRAME)
        elif hasattr(self, "to_dataframe"):
            # Fallback to existing method
            return self.to_dataframe()
        else:
            raise NotImplementedError(f"No serializer registered for {type(self)}")

    def serialize_to_series(self) -> pd.Series:
        """Serialize to Series for time series export"""
        if self._serializer:
            return self._serializer.serialize(self, SerializationMode.SERIES)
        else:
            raise NotImplementedError(f"No series serializer for {type(self)}")

    def serialize_to_dict(self) -> dict:
        """Serialize to dictionary for custom export formats"""
        if self._serializer:
            return self._serializer.serialize(self, SerializationMode.DICT_EXPORT)
        else:
            return self.model_dump()

    @classmethod
    def deserialize_from_dataframe(cls, df: pd.DataFrame):
        """Create model from DataFrame"""
        if cls._serializer:
            return cls._serializer.deserialize(df, cls, SerializationMode.DATAFRAME)
        else:
            raise NotImplementedError(f"No deserializer for {cls}")
