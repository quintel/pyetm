"""Guards the public API surface exposed via ``pyetm.__all__``."""

import pyetm


def test_all_names_are_importable():
    for name in pyetm.__all__:
        assert hasattr(pyetm, name), f"pyetm.__all__ lists {name!r} but it is not importable"


def test_all_has_no_duplicates():
    assert len(pyetm.__all__) == len(set(pyetm.__all__))
