import pytest
from pathlib import Path
import tempfile
import shutil
from unittest.mock import patch

from pyetm.utils.paths import PyetmPaths


class TestPyetmPaths:

    @pytest.fixture
    def temp_project_structure(self):
        """Create a temporary project structure for testing."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create project structure
            project_root = temp_path / "project"
            project_root.mkdir()

            inputs_dir = project_root / "inputs"
            outputs_dir = project_root / "outputs"
            inputs_dir.mkdir()
            outputs_dir.mkdir()

            # Create some test files
            (inputs_dir / "test_file.txt").write_text("test content")
            (inputs_dir / "subdir").mkdir()
            (inputs_dir / "subdir" / "nested_file.txt").write_text("nested content")

            yield {
                "temp_dir": temp_path,
                "project_root": project_root,
                "inputs_dir": inputs_dir,
                "outputs_dir": outputs_dir,
            }

    def test_init_default(self):
        """Test PyetmPaths initialization with default parameters."""
        paths = PyetmPaths()
        assert paths._start == Path.cwd()

    def test_init_with_start_path(self):
        """Test PyetmPaths initialization with custom start path."""
        start_path = Path("/custom/start")
        paths = PyetmPaths(start=start_path)
        assert paths._start == start_path

    def test_init_with_string_start(self):
        """Test PyetmPaths initialization with string start path."""
        start_str = "/custom/start"
        paths = PyetmPaths(start=start_str)
        assert paths._start == Path(start_str)

    def test_find_root_with_existing_dir(self, temp_project_structure):
        """Test _find_root_with when the directory exists."""
        project_root = temp_project_structure["project_root"]

        result = PyetmPaths._find_root_with("inputs", project_root)
        assert result == project_root

    def test_find_root_with_nonexistent_dir(self):
        """Test _find_root_with when directory doesn't exist."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            result = PyetmPaths._find_root_with("nonexistent", temp_path)
            # Should return the start directory when nothing is found
            assert result == temp_path

    def test_find_root_with_parent_search(self, temp_project_structure):
        """Test _find_root_with searches parent directories."""
        project_root = temp_project_structure["project_root"]
        subdir = project_root / "deep" / "nested" / "path"
        subdir.mkdir(parents=True)

        result = PyetmPaths._find_root_with("inputs", subdir)
        assert result == project_root

    def test_find_root_with_permission_error(self):
        """Test _find_root_with handles permission errors gracefully."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Mock Path.exists to raise an exception
            with patch.object(
                Path, "exists", side_effect=PermissionError("Access denied")
            ):
                result = PyetmPaths._find_root_with("inputs", temp_path)
                assert result == temp_path

    def test_inputs_dir(self, temp_project_structure):
        """Test inputs_dir method."""
        project_root = temp_project_structure["project_root"]
        paths = PyetmPaths(start=project_root)

        result = paths.inputs_dir()
        assert result == project_root / "inputs"
        assert result.exists()

    def test_outputs_dir(self, temp_project_structure):
        """Test outputs_dir method."""
        project_root = temp_project_structure["project_root"]
        paths = PyetmPaths(start=project_root)

        result = paths.outputs_dir()
        assert result == project_root / "outputs"
        assert result.exists()

    def test_resolve_for_read_absolute_path(self):
        """Test resolve_for_read with absolute path."""
        paths = PyetmPaths()
        abs_path = Path("/absolute/path/file.txt")

        result = paths.resolve_for_read(abs_path)
        assert result == abs_path

    def test_resolve_for_read_existing_relative_path(self, temp_project_structure):
        """Test resolve_for_read with existing relative path."""
        project_root = temp_project_structure["project_root"]
        paths = PyetmPaths(start=project_root)

        # Create a file in the current directory relative to project_root
        test_file = project_root / "local_file.txt"
        test_file.write_text("local content")

        with patch("pathlib.Path.cwd", return_value=project_root):
            result = paths.resolve_for_read("local_file.txt")
            assert result == Path("local_file.txt")

    def test_resolve_for_read_nonexistent_relative_path_found_in_inputs(
        self, temp_project_structure
    ):
        """Test resolve_for_read with nonexistent relative path that exists in inputs."""
        project_root = temp_project_structure["project_root"]
        paths = PyetmPaths(start=project_root)

        result = paths.resolve_for_read("test_file.txt")
        assert result == project_root / "inputs" / "test_file.txt"
        assert result.exists()

    def test_resolve_for_read_nonexistent_relative_path_not_found(
        self, temp_project_structure
    ):
        """Test resolve_for_read with nonexistent relative path that doesn't exist anywhere."""
        project_root = temp_project_structure["project_root"]
        paths = PyetmPaths(start=project_root)

        result = paths.resolve_for_read("nonexistent.txt")
        # Should return the original path when not found
        assert result == Path("nonexistent.txt")

    def test_resolve_for_read_with_subdirectory(self, temp_project_structure):
        """Test resolve_for_read with subdirectory paths."""
        project_root = temp_project_structure["project_root"]
        paths = PyetmPaths(start=project_root)

        result = paths.resolve_for_read("subdir/nested_file.txt")
        assert result == project_root / "inputs" / "subdir" / "nested_file.txt"
        assert result.exists()

    def test_resolve_for_read_custom_default_dir(self, temp_project_structure):
        """Test resolve_for_read with custom default directory."""
        project_root = temp_project_structure["project_root"]
        custom_dir = project_root / "custom"
        custom_dir.mkdir()
        (custom_dir / "custom_file.txt").write_text("custom content")

        paths = PyetmPaths(start=project_root)
        result = paths.resolve_for_read("custom_file.txt", default_dir="custom")
        assert result == custom_dir / "custom_file.txt"
        assert result.exists()

    def test_resolve_for_read_string_input(self, temp_project_structure):
        """Test resolve_for_read with string input."""
        project_root = temp_project_structure["project_root"]
        paths = PyetmPaths(start=project_root)

        result = paths.resolve_for_read("test_file.txt")
        assert result == project_root / "inputs" / "test_file.txt"
        assert result.exists()

    def test_resolve_for_write_absolute_path(self):
        """Test resolve_for_write with absolute path."""
        with tempfile.TemporaryDirectory() as temp_dir:
            paths = PyetmPaths()
            abs_path = Path(temp_dir) / "output.txt"

            result = paths.resolve_for_write(abs_path)
            assert result == abs_path
            # Parent should be created
            assert result.parent.exists()

    def test_resolve_for_write_absolute_path_no_create_parents(self):
        """Test resolve_for_write with absolute path and create_parents=False."""
        with tempfile.TemporaryDirectory() as temp_dir:
            paths = PyetmPaths()
            abs_path = Path(temp_dir) / "nested" / "deep" / "output.txt"

            result = paths.resolve_for_write(abs_path, create_parents=False)
            assert result == abs_path
            # Parent should NOT be created
            assert not result.parent.exists()

    def test_resolve_for_write_relative_path(self, temp_project_structure):
        """Test resolve_for_write with relative path."""
        project_root = temp_project_structure["project_root"]
        paths = PyetmPaths(start=project_root)

        result = paths.resolve_for_write("output.txt")
        assert result == project_root / "outputs" / "output.txt"
        assert result.parent.exists()

    def test_resolve_for_write_relative_path_with_subdirs(self, temp_project_structure):
        """Test resolve_for_write with relative path containing subdirectories."""
        project_root = temp_project_structure["project_root"]
        paths = PyetmPaths(start=project_root)

        result = paths.resolve_for_write("subdir/nested/output.txt")
        expected = project_root / "outputs" / "subdir" / "nested" / "output.txt"
        assert result == expected
        assert result.parent.exists()

    def test_resolve_for_write_custom_default_dir(self, temp_project_structure):
        """Test resolve_for_write with custom default directory."""
        project_root = temp_project_structure["project_root"]
        paths = PyetmPaths(start=project_root)

        result = paths.resolve_for_write("output.txt", default_dir="custom")
        expected = project_root / "custom" / "output.txt"
        assert result == expected
        assert result.parent.exists()

    def test_resolve_for_write_string_input(self, temp_project_structure):
        """Test resolve_for_write with string input."""
        project_root = temp_project_structure["project_root"]
        paths = PyetmPaths(start=project_root)

        result = paths.resolve_for_write("output.txt")
        assert result == project_root / "outputs" / "output.txt"
        assert result.parent.exists()

    def test_pathlike_type_annotation(self):
        """Test that PathLikeOrStr type annotation works correctly."""
        paths = PyetmPaths()

        # Test with string
        result1 = paths.resolve_for_read("test.txt")
        assert isinstance(result1, Path)

        # Test with Path
        result2 = paths.resolve_for_read(Path("test.txt"))
        assert isinstance(result2, Path)

    def test_edge_case_empty_string(self, temp_project_structure):
        """Test edge case with empty string path."""
        project_root = temp_project_structure["project_root"]
        paths = PyetmPaths(start=project_root)

        result = paths.resolve_for_read("")
        # Empty string should be treated as current directory
        assert result == Path("")

    def test_edge_case_dot_path(self, temp_project_structure):
        """Test edge case with dot (current directory) path."""
        project_root = temp_project_structure["project_root"]
        paths = PyetmPaths(start=project_root)

        with patch("pathlib.Path.cwd", return_value=project_root):
            result = paths.resolve_for_read(".")
            assert result == Path(".")

    def test_complex_project_structure(self):
        """Test with a more complex, realistic project structure."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create nested project structure
            project = temp_path / "my_project"
            project.mkdir()
            (project / "inputs").mkdir()
            (project / "outputs").mkdir()

            src = project / "src" / "deep" / "nested"
            src.mkdir(parents=True)

            # Test from deep nested location
            paths = PyetmPaths(start=src)

            inputs_dir = paths.inputs_dir()
            outputs_dir = paths.outputs_dir()

            assert inputs_dir == project / "inputs"
            assert outputs_dir == project / "outputs"

    @pytest.mark.parametrize(
        "path_input,expected_type",
        [
            ("string_path.txt", str),
            (Path("path_object.txt"), Path),
        ],
    )
    def test_path_input_types(self, path_input, expected_type, temp_project_structure):
        """Test that both string and Path inputs are handled correctly."""
        project_root = temp_project_structure["project_root"]
        paths = PyetmPaths(start=project_root)

        result_read = paths.resolve_for_read(path_input)
        result_write = paths.resolve_for_write(path_input)

        assert isinstance(result_read, Path)
        assert isinstance(result_write, Path)

    def test_concurrent_access_safety(self, temp_project_structure):
        """Test that the class behaves correctly with multiple instances."""
        project_root = temp_project_structure["project_root"]

        paths1 = PyetmPaths(start=project_root)
        paths2 = PyetmPaths(start=project_root / "inputs")

        result1 = paths1.resolve_for_read("test_file.txt")
        result2 = paths2.resolve_for_read("test_file.txt")

        # Both should work independently
        assert isinstance(result1, Path)
        assert isinstance(result2, Path)
