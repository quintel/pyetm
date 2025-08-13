from pathlib import Path
import re
import yaml, os
from typing import Optional, ClassVar, List, Annotated
from pydantic import Field, ValidationError, HttpUrl, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parents[3]
CONFIG_FILE = PROJECT_ROOT / "config.yml"


class AppConfig(BaseSettings):
    """
    Application configuration loaded from YAML.
    """

    etm_api_token: Annotated[
        str,
        Field(
            ...,
            description="Your ETM API token: must be either `etm_<JWT>` or `etm_beta_<JWT>`. If not set please set $ETM_API_TOKEN or config.yml:etm_api_token",
        ),
    ]
    base_url: HttpUrl = Field(
        "https://engine.energytransitionmodel.com/api/v3",
        description="Base URL for the ETM API",
    )
    environment: Optional[str] = Field(
        None,
        description=(
            "ETM environment to target. One of: 'pro' (default), 'beta', 'local', or a stable tag 'YYYY-MM'. "
            "When set and base_url is not provided, base_url will be inferred."
        ),
    )
    log_level: Optional[str] = Field(
        "INFO",
        description="App logging level",
    )

    model_config: ClassVar[SettingsConfigDict] = SettingsConfigDict(
        env_file=None, extra="ignore", case_sensitive=False
    )

    temp_folder: Optional[Path] = PROJECT_ROOT / "tmp"

    @field_validator("etm_api_token")
    @classmethod
    def check_jwt(cls, v: str) -> str:
        # prefix and optional 'beta'
        if v.startswith("etm_beta_"):
            body = v[len("etm_beta_") :]
        elif v.startswith("etm_"):
            body = v[len("etm_") :]
        else:
            raise ValueError(
                "Invalid ETM API token: must start with 'etm_' or 'etm_beta_'"
            )

        # body must start with an alphanumeric character (no double underscore)
        if not body or not body[0].isalnum():
            raise ValueError(
                "Invalid ETM API token: JWT body must start with an alphanumeric character"
            )

        # must have exactly three dot-separated segments
        segs = body.split(".")
        if len(segs) != 3:
            raise ValueError(
                "Invalid ETM API token: JWT must have exactly three segments separated by '.'"
            )

        # no spaces in any segment
        if any(" " in seg for seg in segs):
            raise ValueError(
                "Invalid ETM API token: JWT segments must not contain spaces"
            )

        return v

    def path_to_tmp(self, subfolder: str):
        folder = self.temp_folder / subfolder
        folder.mkdir(parents=True, exist_ok=True)
        return folder

    @classmethod
    def from_yaml(cls, path: Path) -> "AppConfig":
        raw = {}
        if path.is_file():
            try:
                raw = yaml.safe_load(path.read_text()) or {}
            except yaml.YAMLError:
                raw = {}

        data = {k.lower(): v for k, v in raw.items()}

        # Collect environment variables overriding YAML
        for field in ("etm_api_token", "base_url", "log_level", "environment"):
            if val := os.getenv(field.upper()):
                data[field] = val

        # If base_url wasn't explicitly provided, infer it from environment if present
        if "base_url" not in data or not data["base_url"]:
            env = (data.get("environment") or "").strip().lower()
            if env:
                data["base_url"] = _infer_base_url_from_env(env)

        return cls(**data)


def get_settings() -> AppConfig:
    """
    Always re-load AppConfig from disk and ENV on each call,
    and raise a clear, aggregated message if anything required is missing.
    """
    try:
        return AppConfig.from_yaml(CONFIG_FILE)
    except ValidationError as exc:
        missing_or_invalid: List[str] = []
        for err in exc.errors():
            loc = ".".join(str(x) for x in err["loc"])
            msg = err["msg"]
            missing_or_invalid.append(f"• {loc}: {msg}")

        detail = "\n".join(missing_or_invalid)
        raise RuntimeError(
            f"\nConfiguration error: one or more required settings are missing or invalid:\n\n"
            f"{detail}\n\n"
            f"Please set them via environment variables or in `{CONFIG_FILE}`."
        ) from exc


def _infer_base_url_from_env(environment: str) -> str:
    """
    Infers the ETM API base URL from an environment string.

    Supported values (case-insensitive):
      - 'pro'/'prod' (default): https://engine.energytransitionmodel.com/api/v3
      - 'beta'/'staging':       https://beta.engine.energytransitionmodel.com/api/v3
      - 'local'/'dev'/'development': http://localhost:3000/api/v3
      - stable tags 'YYYY-MM':  https://{YYYY-MM}.engine.energytransitionmodel.com/api/v3

    Falls back to the 'pro' URL if the input is empty or unrecognized.
    """
    env = (environment or "").strip().lower()

    if env in ("", "pro", "prod"):  # default
        return "https://engine.energytransitionmodel.com/api/v3"
    if env in ("beta", "staging"):
        return "https://beta.engine.energytransitionmodel.com/api/v3"
    if env in ("local", "dev", "development"):
        return "http://localhost:3000/api/v3"

    # Stable tagged environments e.g., '2025-01'
    if re.fullmatch(r"\d{4}-\d{2}", env):
        return f"https://{env}.engine.energytransitionmodel.com/api/v3"

    # Unrecognized: be conservative and return production
    return "https://engine.energytransitionmodel.com/api/v3"
