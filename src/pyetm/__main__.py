"""
PyETM Command Line Interface
Provides CLI commands for managing pyetm projects.
"""

from __future__ import annotations

import sys
from pathlib import Path
from importlib.metadata import version
import shutil
import click


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
        overwrite = click.confirm(
            f"  {path.name} already exists. Overwrite?", default=False
        )
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


def copy_example_file(
    target_dir: Path, force: bool = False
) -> tuple[bool, bool]:
    """
    Copy example Excel file from the package to the target directory.

    Args:
        target_dir: Directory to copy file into
        force: If True, overwrite existing files without prompting

    Returns:
        Tuple of (created, skipped) booleans
    """
    examples_path = get_examples_path()

    if not examples_path.exists():
        raise FileNotFoundError(
            f"Examples directory not found: {examples_path}\n"
            "This may indicate an incomplete package installation."
        )

    source_path = examples_path / "inputs" / "example_input_excel.xlsx"
    dest_path = target_dir / "inputs" / "example_input_excel.xlsx"

    if not source_path.exists():
        click.echo(f"  Warning: Example Excel file not found: {source_path.name}")
        return False, False

    # Create parent directory if needed
    dest_path.parent.mkdir(parents=True, exist_ok=True)

    # Check if file already exists
    if dest_path.exists() and not force:
        overwrite = click.confirm(
            f"  {dest_path.name} already exists. Overwrite?", default=False
        )
        if not overwrite:
            return False, True

    # Copy the file
    shutil.copy2(source_path, dest_path)
    return True, False


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
def init(environment: str | None, log_level: str, force: bool) -> None:
    """
    Initialize a new pyetm project.

    Creates .env configuration file and copies example Excel file to the current directory.
    """
    click.echo("\nSetting up pyetm...\n")

    # Prompt for environment if not provided
    if not environment:
        click.echo("Common environments: pro, beta, local, tyndp2024, 2025-01")
        environment = click.prompt(
            "Environment",
            default="pro",
            type=str,
        )

    # Ensure environment is not None for type checking
    assert environment is not None, "Environment must be specified"

    # Determine target directory (current working directory)
    target_dir = Path.cwd()

    # Track what was created
    created_files = []

    # 1. Create .env file
    try:
        env_template = read_template(".env.template")

        # Replace placeholders
        env_content = env_template.replace("{{ENVIRONMENT}}", environment)
        env_content = env_content.replace("{{LOG_LEVEL}}", log_level)

        # Handle optional BASE_URL line
        env_content = env_content.replace("{{BASE_URL_LINE}}", "# BASE_URL=")

        # Write .env file
        env_path = target_dir / ".env"
        if write_file_safely(env_path, env_content, force):
            click.echo("✓ Created .env")
            created_files.append(".env")

    except Exception as e:
        click.echo(f"✗ Failed to create .env: {e}", err=True)
        sys.exit(1)

    # 2. Copy example Excel file
    try:
        example_created, example_skipped = copy_example_file(target_dir, force)

        if example_created:
            click.echo("✓ Created inputs/example_input_excel.xlsx")
            created_files.append("inputs/example_input_excel.xlsx")
        elif example_skipped:
            click.echo("⊗ Skipped inputs/example_input_excel.xlsx (already exists)")

    except Exception as e:
        click.echo(f"✗ Failed to copy example file: {e}", err=True)
        # Don't exit - .env is more important

    # Next steps
    click.echo("\nNext steps:")
    click.echo("  1. Edit .env and set your environment variables:")
    click.echo("     • ETM_API_TOKEN (get from https://docs.energytransitionmodel.com/api/authentication)")
    click.echo("     • BASE_URL (if using custom ETM instance)")
    click.echo("")
    click.echo("  2. Use pyetm in your Python scripts:")
    click.echo("     • See: https://quintel.github.io/pyetm/examples/")
    click.echo("")
    click.echo("  3. Check the documentation:")
    click.echo("     • Getting started: https://quintel.github.io/pyetm/")
    click.echo("     • API reference: https://quintel.github.io/pyetm/api/")
    click.echo("")
    click.echo("Remember: Never commit your .env file to version control!\n")


def main() -> None:
    """Entry point for the CLI."""
    cli()


if __name__ == "__main__":
    main()
