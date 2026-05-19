"""
PyETM Command Line Interface
Provides CLI commands for managing pyetm projects.
"""

import sys
from pathlib import Path
from importlib.metadata import version
import shutil
import click
import re


def validate_token(token: str) -> tuple[bool, str]:
    """
    Validate ETM API token format.

    Accepts empty tokens (returns True) to allow unauthenticated mode.
    For non-empty tokens, validates etm_ prefix and basic format.

    Returns:
        Tuple of (is_valid, error_message)
    """
    # Empty token is valid (unauthenticated mode)
    if not token:
        return True, ""

    # Token must start with etm_ prefix
    if not token.startswith("etm_"):
        return False, "Token must start with 'etm_' prefix"

    # Extract body (handle both etm_ and etm_beta_ prefixes)
    if token.startswith("etm_beta_"):
        body = token[len("etm_beta_") :]
    else:
        body = token[len("etm_") :]

    # Body must not be empty and must start with alphanumeric
    if not body or not body[0].isalnum():
        return False, "Token body must start with an alphanumeric character"

    # Relaxed validation: allow non-JWT tokens
    # Only validate JWT structure if it looks like a JWT (has 3 dot-separated segments)
    segs = body.split(".")
    if len(segs) == 3:
        # Validate JWT format
        if any(" " in seg for seg in segs):
            return False, "JWT segments must not contain spaces"
    elif "." in body:
        # Has dots but not exactly 3 segments - might be malformed JWT
        return False, "JWT must have exactly three segments separated by '.'"
    # else: non-JWT token, accept it

    return True, ""


def get_template_path(filename: str) -> Path:
    """Get the path to a template file."""
    # When installed, templates are in the package directory
    package_dir = Path(__file__).parent
    template_path = package_dir / "templates" / filename
    return template_path


def read_template(filename: str) -> str:
    """Read a template file from the package."""
    template_path = get_template_path(filename)
    if not template_path.exists():
        raise FileNotFoundError(
            f"Template file not found: {template_path}\n"
            "This may indicate an incomplete package installation."
        )
    return template_path.read_text(encoding="utf-8")


def write_file_safely(path: Path, content: str, force: bool = False) -> bool:
    """
    Write content to a file with overwrite protection.

    Returns:
        True if file was written, False if skipped
    """
    if path.exists() and not force:
        overwrite = click.confirm(f"  {path.name} already exists. Overwrite?", default=False)
        if not overwrite:
            click.echo(f"  ⊗ Skipped {path.name}")
            return False

    path.write_text(content, encoding="utf-8")
    return True


def get_examples_path() -> Path:
    """Get the path to the examples directory in the package."""
    # In development: src/pyetm/__main__.py -> go up to project root
    # In installed package: site-packages/pyetm/__main__.py -> examples in parent
    package_dir = Path(__file__).parent.parent
    examples_path = package_dir / "examples"

    # If not found (development mode), check project root
    if not examples_path.exists():
        project_root = package_dir.parent
        examples_path = project_root / "examples"

    return examples_path


def copy_example_files(target_dir: Path, force: bool = False) -> tuple[list[str], list[str]]:
    """
    Copy example files from the package to the target directory.

    Files are copied with a flat structure:
    - Notebooks and .py files go to the root of target_dir
    - Excel files go to target_dir/inputs/

    Args:
        target_dir: Directory to copy files into
        force: If True, overwrite existing files without prompting

    Returns:
        Tuple of (created_files, skipped_files)
    """
    examples_path = get_examples_path()

    if not examples_path.exists():
        raise FileNotFoundError(
            f"Examples directory not found: {examples_path}\n"
            "This may indicate an incomplete package installation."
        )

    created_files = []
    skipped_files = []

    # Files to copy with their target paths
    files_to_copy = [
        (
            examples_path / "advanced_features.ipynb",
            target_dir / "advanced_features.ipynb",
        ),
        (examples_path / "basic_features.ipynb", target_dir / "basic_features.ipynb"),
        (examples_path / "example_helpers.py", target_dir / "example_helpers.py"),
        (
            examples_path / "inputs" / "example_input_excel.xlsx",
            target_dir / "inputs" / "example_input_excel.xlsx",
        ),
    ]

    for source_path, dest_path in files_to_copy:
        if not source_path.exists():
            click.echo(f"  ⚠ Warning: Source file not found: {source_path.name}")
            continue

        # Create parent directory if needed (for inputs folder)
        dest_path.parent.mkdir(parents=True, exist_ok=True)

        # Check if file already exists
        if dest_path.exists() and not force:
            overwrite = click.confirm(
                f"  {dest_path.name} already exists. Overwrite?", default=False
            )
            if not overwrite:
                click.echo(f"  ⊗ Skipped {dest_path.name}")
                skipped_files.append(dest_path.name)
                continue

        # Copy the file
        shutil.copy2(source_path, dest_path)
        created_files.append(dest_path.name)

    return created_files, skipped_files


@click.group()
@click.version_option(version=version("pyetm"), prog_name="pyetm")
def cli() -> None:
    """
    PyETM - Python client for the Energy Transition Model API

    Get started with: pyetm init
    """
    pass


@cli.command()
@click.option(
    "--token",
    default=None,
    help="ETM API token (optional, format: etm_<JWT> or etm_beta_<JWT>). Leave empty for unauthenticated access to public scenarios only.",
)
@click.option(
    "--environment",
    default=None,
    help="ETM environment to target (e.g., 'pro', 'beta', 'local', 'tyndp2024', '2025-01'). Defaults to 'pro'.",
)
@click.option(
    "--log-level",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]),
    default="INFO",
    help="Logging level",
)
@click.option(
    "--force",
    is_flag=True,
    help="Overwrite existing files without prompting",
)
def init(token: str | None, environment: str | None, log_level: str, force: bool) -> None:
    """
    Initialize a new pyetm project.

    Creates .env configuration file and copies example notebooks and files to the current directory.
    """
    click.echo("\n" + "=" * 60)
    click.echo("PyETM Project Initialization")
    click.echo("=" * 60 + "\n")

    # Prompt for token if not provided via CLI
    if token is None:
        click.echo("Please paste your ETM API token below and press Enter:")
        click.echo("(Leave empty and press Enter to skip for unauthenticated access)\n")
        try:
            token = input("ETM API Token (optional): ").strip()
        except EOFError:
            token = ""

    # Prompt for environment if not provided
    if not environment:
        click.echo("\nCommon environments: pro, beta, local, tyndp2024, 2025-01")
        environment = click.prompt(
            "Environment",
            default="pro",
            type=str,
        )

    # Validate token
    is_valid, error_msg = validate_token(token)
    if not is_valid:
        click.echo(f"✗ Invalid API token: {error_msg}", err=True)
        click.echo(
            "\nGet your token from: https://docs.energytransitionmodel.com/api/authentication",
            err=True,
        )
        sys.exit(1)

    if token:
        click.echo("Token validated\n")
    else:
        click.echo("No token provided - you will only be able to access public scenarios\n")

    # Determine target directory (current working directory)
    target_dir = Path.cwd()
    click.echo(f"Creating files in: {target_dir}\n")

    # Track what was created
    created_files = []
    skipped_files = []

    # 1. Create .env file
    click.echo("Creating .env configuration...")
    try:
        env_template = read_template(".env.template")

        # Replace placeholders
        # If token is empty, comment out the ETM_API_TOKEN line
        if token:
            env_content = env_template.replace("{{ETM_API_TOKEN}}", token)
        else:
            env_content = env_template.replace(
                "ETM_API_TOKEN={{ETM_API_TOKEN}}", "# ETM_API_TOKEN="
            )

        env_content = env_content.replace("{{ENVIRONMENT}}", environment)
        env_content = env_content.replace("{{LOG_LEVEL}}", log_level)

        # Handle optional BASE_URL line
        env_content = env_content.replace("{{BASE_URL_LINE}}", "# BASE_URL=")

        # Write .env file
        env_path = target_dir / ".env"
        if write_file_safely(env_path, env_content, force):
            click.echo(f"  ✓ Created {env_path.name}")
            created_files.append(".env")
        else:
            skipped_files.append(".env")

    except Exception as e:
        click.echo(f"  ✗ Failed to create .env: {e}", err=True)
        sys.exit(1)

    # 2. Copy example files
    click.echo("\nCopying example files...")
    try:
        example_created, example_skipped = copy_example_files(target_dir, force)

        for filename in example_created:
            click.echo(f"  ✓ Created {filename}")
            created_files.append(filename)

        skipped_files.extend(example_skipped)

    except Exception as e:
        click.echo(f"  ✗ Failed to copy example files: {e}", err=True)
        # Don't exit - .env is more important

    # Success summary
    click.echo("\n" + "=" * 60)
    click.echo("Initialization complete!")
    click.echo("=" * 60 + "\n")

    if created_files:
        click.echo("Created files:")
        for filename in created_files:
            click.echo(f"  • {filename}")

    if skipped_files:
        click.echo("\nSkipped files (already exist):")
        for filename in skipped_files:
            click.echo(f"  • {filename}")

    # Next steps
    click.echo("\nNext steps:")
    click.echo("  1. Review your .env configuration")
    click.echo("  2. Explore the example notebooks:")
    click.echo("     • basic_features.ipynb - Introduction to core functionality")
    click.echo("     • advanced_features.ipynb - Advanced usage patterns")
    click.echo("  3. Check out: https://docs.energytransitionmodel.com/main/pyetm/")

    click.echo("\n  Remember: Never commit your .env file to version control!\n")


def main() -> None:
    """Entry point for the CLI."""
    cli()


if __name__ == "__main__":
    main()
