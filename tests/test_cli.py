"""
Tests for PyETM CLI commands
"""

import pytest
from pathlib import Path
from click.testing import CliRunner
from pyetm.__main__ import cli, validate_token


class TestTokenValidation:
    """Test token validation logic"""

    def test_valid_token_with_etm_prefix(self):
        """Valid token with etm_ prefix"""
        token = "etm_abc123.def456.ghi789"
        is_valid, error_msg = validate_token(token)
        assert is_valid
        assert error_msg == ""

    def test_valid_token_with_beta_prefix(self):
        """Valid token with etm_beta_ prefix"""
        token = "etm_beta_xyz123.abc456.def789"
        is_valid, error_msg = validate_token(token)
        assert is_valid
        assert error_msg == ""

    def test_invalid_token_wrong_prefix(self):
        """Token with invalid prefix"""
        token = "invalid_abc123.def456.ghi789"
        is_valid, error_msg = validate_token(token)
        assert not is_valid
        assert "must start with" in error_msg

    def test_invalid_token_no_prefix(self):
        """Token without prefix"""
        token = "abc123.def456.ghi789"
        is_valid, error_msg = validate_token(token)
        assert not is_valid
        assert "must start with" in error_msg

    def test_invalid_token_double_underscore(self):
        """Token with double underscore (invalid body start)"""
        token = "etm__abc123.def456.ghi789"
        is_valid, error_msg = validate_token(token)
        assert not is_valid
        assert "must start with an alphanumeric" in error_msg

    def test_invalid_token_wrong_segment_count(self):
        """Token with wrong number of segments"""
        token = "etm_abc123.def456"
        is_valid, error_msg = validate_token(token)
        assert not is_valid
        assert "three segments" in error_msg

    def test_invalid_token_too_many_segments(self):
        """Token with too many segments"""
        token = "etm_abc123.def456.ghi789.jkl012"
        is_valid, error_msg = validate_token(token)
        assert not is_valid
        assert "three segments" in error_msg

    def test_invalid_token_with_spaces(self):
        """Token with spaces in segments"""
        token = "etm_abc 123.def456.ghi789"
        is_valid, error_msg = validate_token(token)
        assert not is_valid
        assert "must not contain spaces" in error_msg


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
                input="etm_test.token.here\npro\nINFO\n",
            )

            assert result.exit_code == 0
            assert Path(".env").exists()

            # Check content
            env_content = Path(".env").read_text()
            assert "ETM_API_TOKEN=etm_test.token.here" in env_content
            assert "ENVIRONMENT=pro" in env_content
            assert "LOG_LEVEL=INFO" in env_content

    def test_init_creates_quickstart(self, runner, temp_dir):
        """Test that init creates quickstart.py"""
        with runner.isolated_filesystem(temp_dir=temp_dir):
            result = runner.invoke(
                cli,
                ["init"],
                input="etm_test.token.here\npro\nINFO\n",
            )

            assert result.exit_code == 0
            assert Path("quickstart.py").exists()

            # Check it's a Python file
            quickstart_content = Path("quickstart.py").read_text()
            assert "from pyetm.models import Scenario" in quickstart_content

    def test_init_with_cli_options(self, runner, temp_dir):
        """Test init with command-line options instead of prompts"""
        with runner.isolated_filesystem(temp_dir=temp_dir):
            result = runner.invoke(
                cli,
                [
                    "init",
                    "--token",
                    "etm_beta_cli.test.token",
                    "--environment",
                    "beta",
                    "--log-level",
                    "DEBUG",
                ],
            )

            assert result.exit_code == 0
            env_content = Path(".env").read_text()
            assert "ETM_API_TOKEN=etm_beta_cli.test.token" in env_content
            assert "ENVIRONMENT=beta" in env_content
            assert "LOG_LEVEL=DEBUG" in env_content

    def test_init_invalid_token_exits(self, runner, temp_dir):
        """Test that invalid token causes exit"""
        with runner.isolated_filesystem(temp_dir=temp_dir):
            result = runner.invoke(
                cli,
                ["init"],
                input="invalid_token\npro\n",
            )

            assert result.exit_code != 0
            assert "Invalid API token" in result.output

    def test_init_no_quickstart_flag(self, runner, temp_dir):
        """Test --no-quickstart flag"""
        with runner.isolated_filesystem(temp_dir=temp_dir):
            result = runner.invoke(
                cli,
                [
                    "init",
                    "--token",
                    "etm_test.token.here",
                    "--environment",
                    "pro",
                    "--no-quickstart",
                ],
            )

            assert result.exit_code == 0
            assert Path(".env").exists()
            assert not Path("quickstart.py").exists()

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
                input="etm_test.token.here\npro\nINFO\nn\n",  # 'n' = no to overwrite
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

            quickstart_path = Path("quickstart.py")
            quickstart_path.write_text("# Old quickstart")

            # Run init with --force
            result = runner.invoke(
                cli,
                [
                    "init",
                    "--token",
                    "etm_test.token.here",
                    "--environment",
                    "pro",
                    "--force",
                ],
            )

            assert result.exit_code == 0

            # Files should be overwritten
            env_content = env_path.read_text()
            assert "OLD_CONTENT" not in env_content
            assert "ETM_API_TOKEN=etm_test.token.here" in env_content

            quickstart_content = quickstart_path.read_text()
            assert "# Old quickstart" not in quickstart_content
            assert "from pyetm.models import Scenario" in quickstart_content

    def test_init_success_message(self, runner, temp_dir):
        """Test that success message is shown"""
        with runner.isolated_filesystem(temp_dir=temp_dir):
            result = runner.invoke(
                cli,
                ["init"],
                input="etm_test.token.here\npro\nINFO\n",
            )

            assert result.exit_code == 0
            assert "Initialization complete" in result.output
            assert "Created files:" in result.output
            assert "Never commit your .env file" in result.output


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

    def test_init_help(self):
        """Test init command help"""
        runner = CliRunner()
        result = runner.invoke(cli, ["init", "--help"])

        assert result.exit_code == 0
        assert "Initialize a new pyetm project" in result.output
        assert "--token" in result.output
        assert "--environment" in result.output
        assert "--force" in result.output
