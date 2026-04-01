"""
PyETM Command Line Interface
Provides CLI commands for managing pyetm projects.
"""

import sys
from pathlib import Path
from importlib.metadata import version
import click
import re


def validate_token(token: str) -> tuple[bool, str]:
    """
    Validate ETM API token format.

    Returns:
        Tuple of (is_valid, error_message)
    """
    # Check prefix
    if token.startswith("etm_beta_"):
        body = token[len("etm_beta_") :]
    elif token.startswith("etm_"):
        body = token[len("etm_") :]
    else:
        return False, "Token must start with 'etm_' or 'etm_beta_'"

    # Body must start with alphanumeric
    if not body or not body[0].isalnum():
        return False, "Token body must start with an alphanumeric character"

    # Must have exactly three dot-separated segments
    segs = body.split(".")
    if len(segs) != 3:
        return False, "Token must have exactly three segments separated by '.'"

    # No spaces in any segment
    if any(" " in seg for seg in segs):
        return False, "Token segments must not contain spaces"

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
        overwrite = click.confirm(
            f"  {path.name} already exists. Overwrite?", default=False
        )
        if not overwrite:
            click.echo(f"  ⊗ Skipped {path.name}")
            return False

    path.write_text(content, encoding="utf-8")
    return True


@click.group()
@click.version_option(version=version("pyetm"), prog_name="pyetm")
def cli():
    """
    PyETM - Python client for the Energy Transition Model API

    Get started with: pyetm init
    """
    pass


@cli.command()
@click.option(
    "--token",
    help="ETM API token (format: etm_<JWT> or etm_beta_<JWT>)",
    prompt="ETM API Token",
    hide_input=True,
)
@click.option(
    "--environment",
    type=click.Choice(["pro", "beta", "local"], case_sensitive=False),
    default="pro",
    prompt="Environment",
    help="ETM environment to target",
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
@click.option(
    "--no-quickstart",
    is_flag=True,
    help="Skip creating quickstart.py template",
)
def init(token, environment, log_level, force, no_quickstart):
    """
    Initialize a new pyetm project.

    Creates .env configuration file and quickstart.py template in the current directory.
    """
    click.echo("\n" + "=" * 60)
    click.echo("PyETM Project Initialization")
    click.echo("=" * 60 + "\n")

    # Validate token
    is_valid, error_msg = validate_token(token)
    if not is_valid:
        click.echo(f"✗ Invalid API token: {error_msg}", err=True)
        click.echo(
            "\nGet your token from: https://docs.energytransitionmodel.com/api/authentication",
            err=True,
        )
        sys.exit(1)

    click.echo("✓ Token validated\n")

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
        env_content = env_template.replace("{{ETM_API_TOKEN}}", token)
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

    # 2. Create quickstart.py (unless --no-quickstart)
    if not no_quickstart:
        click.echo("\nCreating quickstart template...")
        try:
            quickstart_template = read_template("quickstart.py.template")

            # Write quickstart.py
            quickstart_path = target_dir / "quickstart.py"
            if write_file_safely(quickstart_path, quickstart_template, force):
                click.echo(f"  ✓ Created {quickstart_path.name}")
                created_files.append("quickstart.py")
            else:
                skipped_files.append("quickstart.py")

        except Exception as e:
            click.echo(f"  ✗ Failed to create quickstart.py: {e}", err=True)
            # Don't exit - .env is more important

    # Success summary
    click.echo("\n" + "=" * 60)
    click.echo("🎉 Initialization complete!")
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

    if "quickstart.py" in created_files:
        click.echo("  2. Run the quickstart: python quickstart.py")
        click.echo("  3. Explore the examples and documentation")
    else:
        click.echo("  2. Start coding with: from pyetm.models import Scenario")
        click.echo("  3. Check out: https://docs.energytransitionmodel.com/main/pyetm/")

    click.echo("\n⚠️  Remember: Never commit your .env file to version control!\n")


def main():
    """Entry point for the CLI."""
    cli()


if __name__ == "__main__":
    main()
