from pathlib import Path
import re
from typing import Optional, ClassVar, List, Annotated
from pydantic import Field, ValidationError, HttpUrl, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parents[3]
ENV_FILE = PROJECT_ROOT / "config.env"


class AppConfig(BaseSettings):
    """
    Application configuration loaded from .env file and environment variables.
    """

    etm_api_token: Annotated[
        str,
        Field(
            ...,
            description="Your ETM API token: must be either `etm_<JWT>` or `etm_beta_<JWT>`",
        ),
    ]
    base_url: Optional[HttpUrl] = Field(
        None,
        description="Base URL for the ETM API (will be inferred from environment if not provided)",
    )
    environment: Optional[str] = Field(
        "pro",
        description=(
            "ETM environment to target. One of: 'pro' (default), 'beta', 'local', or a stable tag 'YYYY-MM'. "
            "When set and base_url is not provided, base_url will be inferred."
        ),
    )
    log_level: Optional[str] = Field(
        "INFO",
        description="App logging level",
    )

    proxy_servers_http: Optional[str] = Field(
        None,
        description="HTTP proxy server URL",
    )
    proxy_servers_https: Optional[str] = Field(
        None,
        description="HTTPS proxy server URL",
    )
    csv_separator: str = Field(
        ",",
        description="CSV file separator character",
    )
    decimal_separator: str = Field(
        ".",
        description="Decimal separator character",
    )

    model_config: ClassVar[SettingsConfigDict] = SettingsConfigDict(
        case_sensitive=False,
        extra="ignore",
    )

    temp_folder: Optional[Path] = PROJECT_ROOT / "tmp"

    def __init__(self, **values):
        """
        This ensures tests can monkeypatch `pyetm.config.settings.ENV_FILE`
        """
        super().__init__(
            _env_file=ENV_FILE,
            _env_file_encoding="utf-8",
            **values,
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

    def model_post_init(self, __context) -> None:
        """Post-initialization to handle base_url inference."""
        if not self.base_url:
            self.base_url = HttpUrl(_infer_base_url_from_env(self.environment))

    def path_to_tmp(self, subfolder: str):
        folder = self.temp_folder / subfolder
        folder.mkdir(parents=True, exist_ok=True)
        return folder

    @property
    def proxy_servers(self) -> dict[str, str]:
        """Return proxy servers as a dictionary"""
        proxies = {}
        if self.proxy_servers_http:
            proxies["http"] = self.proxy_servers_http
        if self.proxy_servers_https:
            proxies["https"] = self.proxy_servers_https
        return proxies


def get_settings() -> AppConfig:
    """
    Load AppConfig from .env file and environment variables.
    """
    try:
        return AppConfig()
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
            f"Please set them via environment variables or in `{ENV_FILE}`."
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
