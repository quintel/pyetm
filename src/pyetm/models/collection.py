"""Collection model for ETM collection management."""

from __future__ import annotations
from datetime import datetime
from typing import Any, Dict, List, Optional
from pydantic import Field, PrivateAttr
from pyetm.models.base import Base
from pyetm.clients import BaseClient, get_client
from pyetm.services.scenario_runners.create_collection import CreateCollectionRunner
from pyetm.services.scenario_runners.update_collection import UpdateCollectionRunner
from pyetm.services.scenario_runners.fetch_collection import FetchCollectionRunner
from pyetm.services.scenario_runners.delete_collection import DeleteCollectionRunner
from pyetm.services.scenario_runners.fetch_user_collections import (
    FetchUserCollectionsRunner,
)


class CollectionError(Exception):
    """Base collection error"""


class Collection(Base):
    """
    Pydantic model for a MyETM Collection (transition path).

    A Collection groups multiple scenarios together, optionally with interpolation
    for multi-year projections.
    """

    id: int = Field(..., description="Unique collection identifier in MyETM")
    title: str = Field(..., description="Title of the collection")
    interpolation: Optional[bool] = Field(
        False, description="Whether this is an interpolated collection"
    )
    area_code: Optional[str] = Field(None, description="Area code for interpolated collections")
    end_year: Optional[int] = Field(None, description="End year for interpolated collections")
    version: Optional[str] = Field(None, description="ETM version")
    owner: Optional[Dict[str, Any]] = Field(None, description="Owner information")
    saved_scenario_ids: Optional[List[int]] = Field(
        default_factory=list, description="List of saved scenario IDs"
    )
    scenario_ids: Optional[List[int]] = Field(
        default_factory=list, description="List of ETEngine scenario IDs"
    )
    collections_app_url: Optional[str] = Field(
        None, description="URL to view collection in app"
    )
    interpolation_params: Optional[Dict[str, Any]] = Field(
        None, description="Interpolation parameters for multi-year collections"
    )
    discarded: Optional[bool] = Field(False, description="Whether collection is soft-deleted")
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    _client: Optional[BaseClient] = PrivateAttr(None)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Collection):
            return NotImplemented
        return self.id == other.id

    def __hash__(self) -> int:
        return hash(self.id)

    @classmethod
    def create(
        cls,
        title: str,
        scenario_ids: Optional[List[int]] = None,
        saved_scenario_ids: Optional[List[int]] = None,
        area_code: Optional[str] = None,
        end_year: Optional[int] = None,
        interpolation: bool = False,
        client: Optional[BaseClient] = None,
        **kwargs: Any,
    ) -> "Collection":
        """
        Create a new Collection in MyETM.

        Args:
            title: Title for the collection (required)
            scenario_ids: List of ETEngine scenario IDs to include
            saved_scenario_ids: List of MyETM saved scenario IDs to include
            area_code: Area code for interpolated collections
            end_year: End year for interpolated collections
            interpolation: Whether this is an interpolated collection
            client: Optional BaseClient instance
            **kwargs: Additional collection parameters

        Returns:
            Collection: The created collection

        Raises:
            CollectionError: If creation fails

        Example:
            collection = Collection.create(
                title="My Collection",
                saved_scenario_ids=[1, 2, 3],
                interpolation=False
            )
        """
        if client is None:
            client = get_client()

        collection_data = {
            "title": title,
            "interpolation": interpolation,
            **kwargs,
        }

        if scenario_ids:
            collection_data["scenario_ids"] = scenario_ids
        if saved_scenario_ids:
            collection_data["saved_scenario_ids"] = saved_scenario_ids
        if area_code:
            collection_data["area_code"] = area_code
        if end_year:
            collection_data["end_year"] = end_year

        result = CreateCollectionRunner.run(client, collection_data)

        if not result.success:
            raise CollectionError(f"Could not create collection: {result.errors}")

        collection = cls.model_validate(result.data)
        collection._client = client

        for warning in result.errors:
            collection.add_warning("base", warning)

        return collection

    @classmethod
    def load(cls, collection_id: int, client: Optional[BaseClient] = None) -> "Collection":
        """
        Load a Collection from MyETM by ID.

        Args:
            collection_id: ID of the collection to load
            client: Optional BaseClient instance

        Returns:
            Collection: The loaded collection

        Raises:
            CollectionError: If collection not found or loading fails

        Example:
            collection = Collection.load(123)
            print(f"Collection: {collection.title}")
        """
        if client is None:
            client = get_client()

        result = FetchCollectionRunner.run(client, collection_id)

        if not result.success:
            raise CollectionError(f"Could not load collection: {result.errors}")

        collection = cls.model_validate(result.data)
        collection._client = client

        for warning in result.errors:
            collection.add_warning("base", warning)

        return collection

    @classmethod
    def load_all(cls, client: Optional[BaseClient] = None) -> List["Collection"]:
        """
        Load all collections for the authenticated user.

        Args:
            client: Optional BaseClient instance

        Returns:
            List[Collection]: List of all user's collections

        Raises:
            CollectionError: If loading fails

        Example:
            collections = Collection.load_all()
            for collection in collections:
                print(f"{collection.id}: {collection.title}")
        """
        if client is None:
            client = get_client()

        result = FetchUserCollectionsRunner.run(client)

        if not result.success:
            raise CollectionError(f"Could not load collections: {result.errors}")

        collections = [cls.model_validate(data) for data in result.data]

        # Store client for future operations
        for collection in collections:
            collection._client = client

        return collections

    def update(self, client: Optional[BaseClient] = None, **kwargs: Any) -> None:
        """
        Update this Collection.

        Args:
            client: Optional BaseClient instance
            **kwargs: Fields to update (title, area_code, end_year, interpolation,
                     discarded, scenario_ids, saved_scenario_ids)

        Raises:
            CollectionError: If update fails

        Example:
            collection = Collection.load(123)
            collection.update(title="Updated Title", saved_scenario_ids=[1, 2, 3, 4])
        """
        if client is None:
            client = get_client()

        result = UpdateCollectionRunner.run(client, self.id, kwargs)

        if not result.success:
            raise CollectionError(f"Could not update collection: {result.errors}")

        for warning in result.errors:
            self.add_warning("update", warning)

        if result.data:
            for field, value in result.data.items():
                if hasattr(self, field):
                    setattr(self, field, value)

        for field, value in kwargs.items():
            if hasattr(self, field) and (not result.data or field not in result.data):
                setattr(self, field, value)

    def delete(self, client: Optional[BaseClient] = None) -> None:
        """
        Delete this Collection from MyETM.

        Warning: This action is irreversible. The collection will be permanently
        removed from MyETM (soft-deleted).

        Args:
            client: Optional BaseClient instance

        Raises:
            CollectionError: If deletion fails

        Example:
            collection = Collection.load(123)
            collection.delete()
        """
        if client is None:
            client = get_client()

        result = DeleteCollectionRunner.run(client, self.id)

        if not result.success:
            raise CollectionError(f"Could not delete collection: {result.errors}")

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert collection to dictionary.

        Returns:
            Dict[str, Any]: Dictionary representation of the collection
        """
        return self.model_dump(mode="python")
