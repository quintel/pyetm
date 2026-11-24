"""Application configuration and settings management."""

from pathlib import Path
import functools
import logging
import re
import tempfile
from typing import Optional, ClassVar, List, Annotated, Any, Literal
from pydantic import AliasChoices, Field, ValidationError, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger("pyetm")

# The Version this release creates Scenarios on. A maintenance branch pins its stable tag here.
ETM_VERSION = "latest"

MYETM_URLS = {
    "pro": "https://my.energytransitionmodel.com/api/v4",
    "beta": "https://beta.my.energytransitionmodel.com/api/v4",
    "local": "https://myetm.local.energytransitionmodel.com/api/v4",
}
ENVIRONMENT_ALIASES = {
    "prod": "pro",
    "production": "pro",
    "staging": "beta",
    "dev": "local",
    "development": "local",
}


class AppConfig(BaseSettings):
    """
    Application configuration loaded from .env file and environment variables.
    """

    etm_api_token: Annotated[
        Optional[str],
        Field(
            None,
            description=(
                "Your ETM API token (optional): must start with 'etm_' prefix. "
                "If not provided, you will only be able to access public scenarios without authentication."
            ),
        ),
    ]
    environment: Optional[str] = Field(
        "pro",
        description="Which ETM to talk to: 'pro' (default), 'beta' or 'local'",
        # TEMPORARY(2.2): also read a bare ENVIRONMENT
        validation_alias=AliasChoices("ETM_ENVIRONMENT", "ENVIRONMENT"),
    )
    # TEMPORARY(2.2): read only to warn that it is ignored
    ignored_base_url: Optional[str] = Field(None, validation_alias="BASE_URL", exclude=True)
    etm_version: str = Field(
        ETM_VERSION,
        description="The Version new Scenarios are created on",
    )
    log_level: Optional[str] = Field(
        "INFO",
        description="App logging level",
    )
    error_mode: Literal["safe", "default", "dangerous"] = Field(
        "default",
        description=(
            "Error handling mode: 'safe' (all warnings raise exceptions), "
            "'default' (smart behavior: raise on errors in single operations, log in bulk), "
            "'dangerous' (nothing raises, all warnings logged only)"
        ),
        validation_alias="PYETM_ERROR_MODE",
    )

    ssl_verify: bool = Field(
        True,
        description="Verify SSL certificates (set to False only for testing with self-signed certificates)",
    )
    trust_env: bool = Field(
        False,
        description="Trust system environment proxy settings (HTTP_PROXY, HTTPS_PROXY, NO_PROXY)",
    )
    ssl_cert_path: Optional[Path] = Field(
        None,
        description="Path to custom CA certificate bundle for SSL verification",
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
        env_file=".env",
        env_file_encoding="utf-8",
    )

    temp_folder: Optional[Path] = Path(tempfile.gettempdir()) / "pyetm"

    @model_validator(mode="after")
    def validate_ssl_cert_path(self) -> "AppConfig":
        """Validate that SSL cert path exists if provided."""
        if self.ssl_cert_path is not None and not self.ssl_cert_path.exists():
            raise ValueError(
                f"SSL certificate file not found: {self.ssl_cert_path}. "
                f"Please provide a valid path to a CA certificate bundle."
            )
        return self

    @field_validator("etm_api_token")
    @classmethod
    def check_jwt(cls, v: Optional[str]) -> Optional[str]:
        # Token is optional - skip validation if None or empty
        if not v:
            return v

        # Token must start with etm_ prefix
        if not v.startswith("etm_"):
            raise ValueError("Invalid ETM API token: must start with 'etm_' prefix")

        # Extract body (handle both etm_ and etm_beta_ prefixes)
        if v.startswith("etm_beta_"):
            body = v[len("etm_beta_") :]
        else:
            body = v[len("etm_") :]

        # Body must not be empty and must start with an alphanumeric character
        if not body or not body[0].isalnum():
            raise ValueError(
                "Invalid ETM API token: token body must start with an alphanumeric character"
            )

        # Relaxed validation: allow non-JWT tokens (any format after etm_ prefix)
        # Only validate JWT structure if it looks like a JWT (has 3 dot-separated segments)
        segs = body.split(".")
        if len(segs) == 3:
            # Validate JWT format
            if any(" " in seg for seg in segs):
                raise ValueError("Invalid ETM API token: JWT segments must not contain spaces")
        elif "." in body:
            # Has dots but not exactly 3 segments - might be malformed JWT
            raise ValueError(
                "Invalid ETM API token: JWT must have exactly three segments separated by '.'"
            )
        # else: non-JWT token, accept it as-is

        return v

    @model_validator(mode="after")
    def target_the_etm(self) -> "AppConfig":
        """Refuse an ETM_ENVIRONMENT this release cannot reach, and warn about a leftover BASE_URL."""
        url = _myetm_url_for(self.environment)
        if self.ignored_base_url:
            logger.warning(
                "BASE_URL is ignored since pyetm 2.1, so pyetm talks to %s. Remove BASE_URL, and set "
                "ETM_ENVIRONMENT to pro, beta or local, or pass BaseClient(base_url=...).",
                url,
            )
        return self

    @property
    def base_url(self) -> str:
        """The ETM API URL that ETM_ENVIRONMENT picks."""
        return _myetm_url_for(self.environment)

    def model_post_init(self, __context: Any) -> None:
        if not self.etm_api_token:
            logger.warning(
                "No ETM_API_TOKEN provided. You will only be able to access public scenarios without authentication."
            )

    def path_to_tmp(self, subfolder: str) -> Path:
        if self.temp_folder is None:
            raise ValueError("temp_folder is not configured")
        folder = self.temp_folder / subfolder
        folder.mkdir(parents=True, exist_ok=True)
        return folder


@functools.lru_cache(maxsize=1)
def get_settings() -> AppConfig:
    """
    Load AppConfig from .env file and environment variables.

    Cached to ensure only one AppConfig instance is created per session.
    To reload configuration after changing environment variables, use reload_configuration().
    """
    try:
        return AppConfig()  # type: ignore[call-arg]
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
            f"Please set them via environment variables (e.g., ETM_API_TOKEN=...) or in a .env file in your working directory."
        ) from exc


def reload_configuration() -> None:
    """
    Clear cached configuration and error policy to reload from environment.

    This function clears both the settings cache (get_settings) and the error policy
    cache (get_error_policy), ensuring that the next call to either function will
    reload configuration from environment variables.

    Use this when you need to change configuration at runtime, for example:

        import os
        os.environ["PYETM_ERROR_MODE"] = "safe"
        reload_configuration()
        # Next call to get_settings() or get_error_policy() will see the new value

    Note: In production code, configuration should typically be set once at startup.
    This function is primarily useful for testing or interactive environments.
    """
    get_settings.cache_clear()
    # Import here to avoid circular dependency
    from pyetm.models.error_policy import get_error_policy
    get_error_policy.cache_clear()


def _myetm_url_for(environment: Optional[str]) -> str:
    """The ETM API URL for an ETM_ENVIRONMENT, or a startup error naming what to set instead."""
    env = (environment or "pro").strip().lower()
    env = ENVIRONMENT_ALIASES.get(env, env)
    if env in MYETM_URLS:
        return MYETM_URLS[env]

    if re.fullmatch(r"\d{4}(-\d{2})?", env):
        raise ValueError(
            f"ETM_ENVIRONMENT={environment} names a stable Version, which this pyETM release cannot reach. "
            f"Install pyetm 2.0.x to work on it, or set ETM_ENVIRONMENT to pro, beta or local."
        )
    raise ValueError(
        f"Unknown ETM_ENVIRONMENT={environment}. Set it to pro, beta or local, "
        f"or pass BaseClient(base_url=...) for another address."
    )
