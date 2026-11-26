"""Collection model for ETM collection management."""

from __future__ import annotations
from datetime import datetime
from typing import Any, Dict, List, Optional
from pydantic import Field, PrivateAttr
from pyetm.models.base import Base
from pyetm.clients import BaseClient, get_client
from pyetm.config.settings import get_settings
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
    A Collection: saved scenarios on one Version, viewed together.
    """

    id: int = Field(..., description="The collection's ID")
    title: str = Field(..., description="Title of the collection")
    version: Optional[str] = Field(None, description="The Version its saved scenarios are on")
    owner: Optional[Dict[str, Any]] = Field(None, description="Owner information")
    saved_scenario_ids: List[int] = Field(
        default_factory=list, description="List of saved scenario IDs"
    )
    collections_app_url: Optional[str] = Field(
        None, description="URL to view collection in app"
    )
    discarded_at: Optional[datetime] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    _client: Optional[BaseClient] = PrivateAttr(None)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Collection):
            return NotImplemented
        return self.id == other.id

    def __hash__(self) -> int:
        return hash(self.id)

    @property
    def client(self) -> BaseClient:
        """The client this collection was loaded with, or the default one."""
        return self._client or get_client()

    @classmethod
    def create(
        cls,
        title: str,
        saved_scenario_ids: List[int],
        version: Optional[str] = None,
        client: Optional[BaseClient] = None,
    ) -> "Collection":
        """
        Create a new Collection.

        Args:
            title: Title for the collection
            saved_scenario_ids: The saved scenarios to include, at least one
            version: The Version they are on, defaulting to ETM_VERSION
            client: Optional BaseClient instance

        Returns:
            Collection: The created collection

        Raises:
            CollectionError: If creation fails

        Example:
            collection = Collection.create(title="My Collection", saved_scenario_ids=[1, 2, 3])
        """
        if client is None:
            client = get_client()

        collection_data = {
            "title": title,
            "saved_scenario_ids": saved_scenario_ids,
            "version": version or get_settings().etm_version,
        }
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
        Load a Collection by ID.

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

        if result.data is None:
            return []

        collections = [cls.model_validate(data) for data in result.data]

        # Store client for future operations
        for collection in collections:
            collection._client = client

        return collections

    def update(self, **kwargs: Any) -> None:
        """
        Update this Collection.

        Args:
            **kwargs: Fields to update: title, saved_scenario_ids

        Raises:
            CollectionError: If update fails

        Example:
            collection = Collection.load(123)
            collection.update(title="Updated Title", saved_scenario_ids=[1, 2, 3, 4])
        """
        result = UpdateCollectionRunner.run(self.client, self.id, kwargs)

        if not result.success:
            raise CollectionError(f"Could not update collection: {result.errors}")

        for warning in result.errors:
            self.add_warning("update", warning)

        for field, value in (result.data or {}).items():
            if field in Collection.model_fields:
                setattr(self, field, value)

    def delete(self) -> None:
        """
        Permanently delete this Collection.

        WARNING: This is a PERMANENT deletion and CANNOT be undone. The collection
        will be permanently removed.

        NOTE: SavedScenarios within the collection are NOT deleted. Only the
        collection itself is removed. The scenarios will remain and
        can still be accessed individually.

        Raises:
            CollectionError: If deletion fails

        Example:
            collection = Collection.load(123)
            collection.delete()  # PERMANENT deletion - cannot be recovered
        """
        result = DeleteCollectionRunner.run(self.client, self.id)

        if not result.success:
            raise CollectionError(f"Could not delete collection: {result.errors}")

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert collection to dictionary.

        Returns:
            Dict[str, Any]: Dictionary representation of the collection
        """
        return self.model_dump(mode="python")
