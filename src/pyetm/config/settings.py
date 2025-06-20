from pathlib import Path
import yaml, os
from typing import Optional, ClassVar, List, Annotated
from pydantic import Field, ValidationError, HttpUrl, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


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
    log_level: Optional[str] = Field(
        "INFO",
        description="App logging level",
    )

    model_config: ClassVar[SettingsConfigDict] = SettingsConfigDict(
        env_file=None, extra="ignore", case_sensitive=False
    )

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

    @classmethod
    def from_yaml(cls, path: Path) -> "AppConfig":
        raw = {}
        if path.is_file():
            try:
                raw = yaml.safe_load(path.read_text()) or {}
            except yaml.YAMLError:
                raw = {}

        data = {k.lower(): v for k, v in raw.items()}

        for field in ("etm_api_token", "base_url", "log_level"):
            if val := os.getenv(field.upper()):
                data[field] = val

        return cls(**data)


PROJECT_ROOT = Path(__file__).resolve().parents[3]
CONFIG_FILE = PROJECT_ROOT / "config.yml"


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
