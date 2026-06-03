"""
Tests for PyETM CLI commands
"""

import pytest
from pathlib import Path
from click.testing import CliRunner
from unittest.mock import Mock, patch, MagicMock
from pyetm.__main__ import cli


class TestCliInit:
    """Test the pyetm init command"""

    @pytest.fixture
    def runner(self):
        """Click test runner"""
        return CliRunner()

    @pytest.fixture
    def temp_dir(self, tmp_path):
        """Temporary directory for test files"""
        return tmp_path

    def test_init_creates_env_file(self, runner, temp_dir):
        """Test that init creates .env file"""
        with runner.isolated_filesystem(temp_dir=temp_dir):
            result = runner.invoke(
                cli,
                ["init"],
                input="pro\n",
            )

            assert result.exit_code == 0
            assert Path(".env").exists()

            # Check content
            env_content = Path(".env").read_text()
            assert "# ETM_API_TOKEN=" in env_content  # Should be commented out
            assert "ENVIRONMENT=pro" in env_content
            assert "LOG_LEVEL=INFO" in env_content

    def test_init_copies_template_file(self, runner, temp_dir):
        """Test that init copies input template Excel file"""
        with runner.isolated_filesystem(temp_dir=temp_dir):
            result = runner.invoke(
                cli,
                ["init"],
                input="pro\n",
            )

            assert result.exit_code == 0

            # Check input template Excel file is copied
            assert Path("excel/input.xlsx").exists()

            # Check inputs directory was created
            assert Path("excel").is_dir()

    def test_init_with_cli_options(self, runner, temp_dir):
        """Test init with command-line options instead of prompts"""
        with runner.isolated_filesystem(temp_dir=temp_dir):
            result = runner.invoke(
                cli,
                [
                    "init",
                    "--environment",
                    "beta",
                    "--log-level",
                    "DEBUG",
                ],
            )

            assert result.exit_code == 0
            env_content = Path(".env").read_text()
            assert "# ETM_API_TOKEN=" in env_content  # Should be commented out
            assert "ENVIRONMENT=beta" in env_content
            assert "LOG_LEVEL=DEBUG" in env_content

    def test_init_prompts_overwrite(self, runner, temp_dir):
        """Test that init prompts before overwriting existing files"""
        with runner.isolated_filesystem(temp_dir=temp_dir):
            # Create existing .env
            env_path = Path(".env")
            env_path.write_text("OLD_CONTENT=true")

            # Run init and decline overwrite
            result = runner.invoke(
                cli,
                ["init"],
                input="pro\nn\n",  # 'n' = no to overwrite
            )

            assert result.exit_code == 0
            assert "already exists" in result.output
            assert "Skipped .env" in result.output

            # Old content should be preserved
            assert "OLD_CONTENT=true" in env_path.read_text()

    def test_init_force_overwrites(self, runner, temp_dir):
        """Test --force flag overwrites without prompting"""
        with runner.isolated_filesystem(temp_dir=temp_dir):
            # Create existing files
            env_path = Path(".env")
            env_path.write_text("OLD_CONTENT=true")

            inputs_dir = Path("excel")
            inputs_dir.mkdir(exist_ok=True)
            input_path = inputs_dir / "input.xlsx"
            input_path.write_text("Old content")

            # Run init with --force
            result = runner.invoke(
                cli,
                [
                    "init",
                    "--environment",
                    "pro",
                    "--force",
                ],
            )

            assert result.exit_code == 0

            # Files should be overwritten
            env_content = env_path.read_text()
            assert "OLD_CONTENT" not in env_content
            assert "# ETM_API_TOKEN=" in env_content  # Should be commented out

            # Input template should be overwritten (check it's not the old text content)
            assert input_path.exists()
            # Excel files are binary, so just check file exists and is not empty
            assert input_path.stat().st_size > 0

    def test_init_prompts_for_template_overwrite(self, runner, temp_dir):
        """Test that init prompts before overwriting existing input template"""
        with runner.isolated_filesystem(temp_dir=temp_dir):
            # Create existing input template file
            inputs_dir = Path("excel")
            inputs_dir.mkdir(exist_ok=True)
            input_path = inputs_dir / "input.xlsx"
            input_path.write_text("Old content")

            # Run init and decline overwrite for the input template
            result = runner.invoke(
                cli,
                ["init"],
                input="pro\nn\n",  # 'n' = no to overwrite
            )

            assert result.exit_code == 0
            assert "already exists" in result.output
            assert "Skipped" in result.output

            # Old content should be preserved
            assert "Old content" in input_path.read_text()

    def test_init_success_message(self, runner, temp_dir):
        """Test that success message is shown"""
        with runner.isolated_filesystem(temp_dir=temp_dir):
            result = runner.invoke(
                cli,
                ["init"],
                input="pro\n",
            )

            assert result.exit_code == 0
            assert "Never commit your .env file" in result.output
            # Should include instructions about environment variables
            assert "Edit .env and set your environment variables" in result.output
            # Should point to documentation
            assert "quintel.github.io/pyetm" in result.output


class TestCliVersion:
    """Test the version option"""

    def test_version_option(self):
        """Test --version flag"""
        runner = CliRunner()
        result = runner.invoke(cli, ["--version"])

        assert result.exit_code == 0
        assert "pyetm" in result.output


class TestCliHelp:
    """Test help text"""

    def test_main_help(self):
        """Test main CLI help"""
        runner = CliRunner()
        result = runner.invoke(cli, ["--help"])

        assert result.exit_code == 0
        assert "Energy Transition Model" in result.output
        assert "init" in result.output
        assert "run" in result.output

    def test_init_help(self):
        """Test init command help"""
        runner = CliRunner()
        result = runner.invoke(cli, ["init", "--help"])

        assert result.exit_code == 0
        assert "Initialize a new pyetm project" in result.output
        assert "--environment" in result.output
        assert "--force" in result.output

    def test_run_help(self):
        """Test run command help"""
        runner = CliRunner()
        result = runner.invoke(cli, ["run", "--help"])

        assert result.exit_code == 0
        assert "Run scenarios from an Excel input file" in result.output
        assert "--output" in result.output


class TestCliRun:
    """Test the pyetm run command"""

    @pytest.fixture
    def runner(self):
        """Click test runner"""
        return CliRunner()

    @pytest.fixture
    def mock_packer(self):
        """Mock ScenarioPacker"""
        packer = Mock()
        packer.to_excel = Mock()
        return packer

    def test_run_requires_input_file(self, runner):
        """Test that run command requires an input file"""
        result = runner.invoke(cli, ["run"])

        assert result.exit_code != 0
        assert "Missing argument" in result.output or "INPUT_PATH" in result.output

    def test_run_input_file_must_exist(self, runner):
        """Test that run command validates input file exists"""
        result = runner.invoke(cli, ["run", "nonexistent.xlsx"])

        assert result.exit_code != 0
        # Click will error about the file not existing

    @patch("pyetm.models.scenario_packer.ScenarioPacker")
    def test_run_basic_execution(
        self, mock_packer_class, runner, mock_packer, tmp_path
    ):
        """Test basic run command execution"""
        # Setup mock
        mock_packer_class.from_excel.return_value = mock_packer

        # Create a dummy input file
        input_file = tmp_path / "input.xlsx"
        input_file.write_bytes(b"fake excel content")

        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(cli, ["run", str(input_file)])

            assert result.exit_code == 0
            assert "Loading scenarios from" in result.output
            assert "Scenarios loaded and updated on ETM" in result.output
            assert "Exporting results to" in result.output
            assert "Results exported successfully" in result.output
            assert "Done!" in result.output

            # Verify ScenarioPacker was called correctly
            mock_packer_class.from_excel.assert_called_once()
            call_args = mock_packer_class.from_excel.call_args
            assert str(call_args[0][0]) == str(input_file)
            assert call_args[1]["update"] is True

            # Verify export was called (with default output path)
            assert mock_packer.to_excel.called

    @patch("pyetm.models.scenario_packer.ScenarioPacker")
    def test_run_with_custom_output(
        self, mock_packer_class, runner, mock_packer, tmp_path
    ):
        """Test run command with custom output path"""
        # Setup mock
        mock_packer_class.from_excel.return_value = mock_packer

        # Create a dummy input file
        input_file = tmp_path / "input.xlsx"
        input_file.write_bytes(b"fake excel content")

        output_file = tmp_path / "custom_output.xlsx"

        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(
                cli, ["run", str(input_file), "--output", str(output_file)]
            )

            assert result.exit_code == 0
            assert "custom_output.xlsx" in result.output

            # Verify export was called with custom output
            mock_packer.to_excel.assert_called_once_with(str(output_file))

    @patch("pyetm.models.scenario_packer.ScenarioPacker")
    def test_run_creates_output_directory(
        self, mock_packer_class, runner, mock_packer, tmp_path
    ):
        """Test that run command creates output directory if needed"""
        # Setup mock
        mock_packer_class.from_excel.return_value = mock_packer

        # Create a dummy input file
        input_file = tmp_path / "input.xlsx"
        input_file.write_bytes(b"fake excel content")

        # Output in a subdirectory that doesn't exist
        output_file = tmp_path / "results" / "output.xlsx"

        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(
                cli, ["run", str(input_file), "--output", str(output_file)]
            )

            assert result.exit_code == 0
            # Directory should be created
            assert output_file.parent.exists()

    @patch("pyetm.models.scenario_packer.ScenarioPacker")
    def test_run_handles_load_error(self, mock_packer_class, runner, tmp_path):
        """Test run command handles errors during scenario loading"""
        # Setup mock to raise error
        mock_packer_class.from_excel.side_effect = ValueError("Invalid Excel format")

        # Create a dummy input file
        input_file = tmp_path / "input.xlsx"
        input_file.write_bytes(b"fake excel content")

        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(cli, ["run", str(input_file)])

            assert result.exit_code != 0
            assert "Error loading scenarios" in result.output
            assert "Invalid Excel format" in result.output

    @patch("pyetm.models.scenario_packer.ScenarioPacker")
    def test_run_handles_export_error(
        self, mock_packer_class, runner, mock_packer, tmp_path
    ):
        """Test run command handles errors during export"""
        # Setup mock
        mock_packer_class.from_excel.return_value = mock_packer
        mock_packer.to_excel.side_effect = IOError("Cannot write file")

        # Create a dummy input file
        input_file = tmp_path / "input.xlsx"
        input_file.write_bytes(b"fake excel content")

        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(cli, ["run", str(input_file)])

            assert result.exit_code != 0
            assert "Error exporting results" in result.output
            assert "Cannot write file" in result.output

    @patch("pyetm.models.scenario_packer.ScenarioPacker")
    def test_run_with_log_level(self, mock_packer_class, runner, mock_packer, tmp_path):
        """Test run command with custom log level"""
        # Setup mock
        mock_packer_class.from_excel.return_value = mock_packer

        # Create a dummy input file
        input_file = tmp_path / "input.xlsx"
        input_file.write_bytes(b"fake excel content")

        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(
                cli, ["run", str(input_file), "--log-level", "DEBUG"]
            )

            assert result.exit_code == 0
            # Command should complete successfully with debug logging

    @patch("pyetm.models.scenario_packer.ScenarioPacker")
    def test_run_with_no_update_flag(
        self, mock_packer_class, runner, mock_packer, tmp_path
    ):
        """Test run command with --no-update flag (read-only mode)"""
        # Setup mock
        mock_packer_class.from_excel.return_value = mock_packer

        # Create a dummy input file
        input_file = tmp_path / "input.xlsx"
        input_file.write_bytes(b"fake excel content")

        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(cli, ["run", str(input_file), "--no-update"])

            assert result.exit_code == 0
            assert "Loading scenarios from" in result.output
            assert "Scenarios loaded (read-only mode)" in result.output
            assert "Exporting results to" in result.output
            assert "Results exported successfully" in result.output

            # Verify ScenarioPacker was called with update=False
            mock_packer_class.from_excel.assert_called_once()
            call_args = mock_packer_class.from_excel.call_args
            assert str(call_args[0][0]) == str(input_file)
            assert call_args[1]["update"] is False

    @patch("pyetm.models.scenario_packer.ScenarioPacker")
    def test_run_without_no_update_updates_scenarios(
        self, mock_packer_class, runner, mock_packer, tmp_path
    ):
        """Test run command without --no-update flag updates scenarios by default"""
        # Setup mock
        mock_packer_class.from_excel.return_value = mock_packer

        # Create a dummy input file
        input_file = tmp_path / "input.xlsx"
        input_file.write_bytes(b"fake excel content")

        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(cli, ["run", str(input_file)])

            assert result.exit_code == 0
            assert "Scenarios loaded and updated on ETM" in result.output

            # Verify ScenarioPacker was called with update=True
            mock_packer_class.from_excel.assert_called_once()
            call_args = mock_packer_class.from_excel.call_args
            assert call_args[1]["update"] is True
