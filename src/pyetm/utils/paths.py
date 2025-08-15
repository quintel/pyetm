from __future__ import annotations
from pathlib import Path
from typing import Optional, Union


PathLikeOrStr = Union[str, Path]


class PyetmPaths:
    """
    - Reads: if given a relative path that does not exist, try under <root>/inputs.
    - Writes: if given a relative path, place it under <root>/outputs.
    - Absolute paths are always respected.
    - Root discovery: walk upwards from CWD and this file's directory to find a
      directory containing the requested subdirectory (e.g., 'inputs' or 'outputs').
    """

    def __init__(self, start: Optional[Path] = None):
        self._start = Path(start) if start else Path.cwd()

    @staticmethod
    def _find_root_with(dir_name: str, start: Optional[Path] = None) -> Path:
        """Find a root directory that contains the given subdirectory name."""
        candidates = []
        base_from_start = Path.cwd() if start is None else Path(start)
        candidates.extend([base_from_start, *base_from_start.parents])

        here = Path(__file__).resolve().parent
        candidates.extend([here, *here.parents])

        for base in candidates:
            probe = base / dir_name
            try:
                if probe.exists() and probe.is_dir():
                    return base
            except Exception:
                continue

        return base_from_start

    def inputs_dir(self) -> Path:
        root = self._find_root_with("inputs", self._start)
        return root / "inputs"

    def outputs_dir(self) -> Path:
        root = self._find_root_with("outputs", self._start)
        return root / "outputs"

    def resolve_for_read(
        self, path: PathLikeOrStr, *, default_dir: str = "inputs"
    ) -> Path:
        p = Path(path)
        if p.is_absolute() or p.exists():
            return p

        base = (
            self.inputs_dir()
            if default_dir == "inputs"
            else self._find_root_with(default_dir, self._start) / default_dir
        )
        # Preserve any sub-paths the user provided
        relative = p if str(p.parent) != "." else Path(p.name)
        candidate = base / relative
        return candidate if candidate.exists() else p

    def resolve_for_write(
        self,
        path: PathLikeOrStr,
        *,
        default_dir: str = "outputs",
        create_parents: bool = True,
    ) -> Path:
        p = Path(path)
        if p.is_absolute():
            if create_parents:
                p.parent.mkdir(parents=True, exist_ok=True)
            return p

        base = (
            self.outputs_dir()
            if default_dir == "outputs"
            else self._find_root_with(default_dir, self._start) / default_dir
        )
        out = base / p
        if create_parents:
            out.parent.mkdir(parents=True, exist_ok=True)
        return out
