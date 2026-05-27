"""
PyETM Command Line Interface
Provides CLI commands for managing pyetm projects.
"""

from __future__ import annotations

import sys
from pathlib import Path
from importlib.metadata import version
import shutil
import logging
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


def copy_input_template(target_dir: Path, force: bool = False) -> tuple[bool, bool]:
    """
    Copy input template Excel file from templates to the target directory.

    Args:
        target_dir: Directory to copy file into
        force: If True, overwrite existing files without prompting

    Returns:
        Tuple of (created, skipped) booleans
    """
    # Get template from package templates directory
    template_path = get_template_path("input_template.xlsx")

    if not template_path.exists():
        raise FileNotFoundError(
            f"Input template not found: {template_path}\n"
            "This may indicate an incomplete package installation."
        )

    # Copy to inputs/input.xlsx in target directory
    dest_path = target_dir / "inputs" / "input.xlsx"

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
    shutil.copy2(template_path, dest_path)
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

    Creates .env configuration file and copies input template Excel file to the current directory.
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

    # 2. Copy input template Excel file
    try:
        template_created, template_skipped = copy_input_template(target_dir, force)

        if template_created:
            click.echo("✓ Created inputs/input.xlsx")
            created_files.append("inputs/input.xlsx")
        elif template_skipped:
            click.echo("⊗ Skipped inputs/input.xlsx (already exists)")

    except Exception as e:
        click.echo(f"✗ Failed to copy input template: {e}", err=True)
        # Don't exit - .env is more important

    # Next steps
    click.echo("\nNext steps:")
    click.echo("  1. Edit .env and set your environment variables:")
    click.echo(
        "     • ETM_API_TOKEN (see https://docs.energytransitionmodel.com/api/authentication)"
    )
    click.echo("     • ENVIRONMENT or BASE_URL (if using stable or other ETM instance)")
    click.echo("")
    click.echo("  2. Edit inputs/input.xlsx with your scenario data")
    click.echo("        (Don't forget to save your changes!)")
    click.echo("")
    click.echo("  3. Run your scenarios:")
    click.echo("     • pyetm run inputs/input.xlsx")
    click.echo("")
    click.echo("  4. Check the documentation:")
    click.echo("     • Full docs: https://quintel.github.io/pyetm/")
    click.echo("")
    click.echo("Remember: Never commit your .env file to version control!\n")


@cli.command()
@click.argument(
    "input_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
)
@click.option(
    "--output",
    "-o",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help="Output Excel file path. Defaults to '{input_stem}_results.xlsx' in the same directory.",
)
@click.option(
    "--log-level",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]),
    default="INFO",
    help="Logging level for the command execution.",
)
def run(
    input_path: Path,
    output: Path | None,
    log_level: str,
) -> None:
    """
    Run scenarios from an Excel input file and export results.

    Reads scenario definitions from INPUT_PATH, updates them on the ETM platform,
    and exports computed results to an Excel file.

    Examples:

      \b
      # Run with default settings (updates scenarios, outputs to input_results.xlsx)
      pyetm run input.xlsx

      \b
      # Specify custom output path
      pyetm run input.xlsx --output results/my_output.xlsx

    The input Excel file should contain sheets for scenario configuration:
    - MAIN: Scenario identifiers and session information
    - EXPORT_CONFIG: Configuration for what data to export
    - Other optional sheets: SLIDER_SETTINGS, CUSTOM_CURVES, SORTABLES, USERS, etc.

    See https://quintel.github.io/pyetm/getting-started/quickstart/#running-scenarios-from-excel for more guidance.
    """
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, log_level),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Import here to avoid slow startup times for --help
    from pyetm.models.scenario_packer import ScenarioPacker

    click.echo(f"\nLoading scenarios from: {input_path}")

    try:
        # Load scenarios from Excel (update=True means push changes to ETM)
        packer = ScenarioPacker.from_excel(input_path, update=True)
        click.echo("✓ Scenarios loaded and updated on ETM")

    except FileNotFoundError as e:
        click.echo(f"✗ Error: Input file not found: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"✗ Error loading scenarios: {e}", err=True)
        logging.exception("Detailed error:")
        sys.exit(1)

    # Determine output path
    if output is None:
        output = input_path.parent / f"{input_path.stem}_results{input_path.suffix}"

    # Ensure output directory exists
    output.parent.mkdir(parents=True, exist_ok=True)

    click.echo(f"\nExporting results to: {output}")

    try:
        # Export results to Excel
        packer.to_excel(str(output))
        click.echo("✓ Results exported successfully")

    except Exception as e:
        click.echo(f"✗ Error exporting results: {e}", err=True)
        logging.exception("Detailed error:")
        sys.exit(1)

    click.echo("\nDone!\n")


def main() -> None:
    """Entry point for the CLI."""
    cli()


if __name__ == "__main__":
    main()
