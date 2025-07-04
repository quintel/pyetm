import pytest
from pyetm.models.sortable_collection import SortableCollection
from pyetm.models.sortable import Sortable


def test_collection_from_json(sortable_collection_json):
    coll = SortableCollection.from_json(sortable_collection_json)

    assert coll
    # 1 (forecast_storage) + 3 (heat_network subtypes) + 1 (hydrogen_supply) = 5
    assert len(coll) == 5

    first = next(iter(coll))
    assert isinstance(first, Sortable)
    assert first.type == "forecast_storage"
    assert first.order == ["fs1", "fs2"]
    assert first.subtype is None

    assert coll.keys() == [
        "forecast_storage",
        "heat_network",
        "heat_network",
        "heat_network",
        "hydrogen_supply",
    ]


def test_as_dict_roundtrip(sortable_collection_json):
    coll = SortableCollection.from_json(sortable_collection_json)
    rebuilt = coll.as_dict()

    assert rebuilt == sortable_collection_json
