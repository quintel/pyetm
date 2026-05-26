"""Application configuration and settings management."""

from pathlib import Path
import functools
import logging
import re
import tempfile
from typing import Optional, ClassVar, List, Annotated, Any
from pydantic import Field, ValidationError, HttpUrl, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger("pyetm")


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

    def model_post_init(self, __context: Any) -> None:
        """Post-initialization to handle base_url inference and token warnings."""
        if not self.base_url:
            self.base_url = HttpUrl(_infer_base_url_from_env(self.environment or "production"))

        # Warn if no token is provided
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


def _infer_base_url_from_env(environment: str) -> str:
    """
    Infers the ETM API base URL from an environment string.

    Supported values (case-insensitive):
      - 'pro'/'prod' (default): https://engine.energytransitionmodel.com/api/v3
      - 'beta'/'staging':       https://beta.engine.energytransitionmodel.com/api/v3
      - 'local'/'dev'/'development': http://localhost:3000/api/v3
      - stable tags 'YYYY-MM':  https://{YYYY-MM}.engine.energytransitionmodel.com/api/v3
      - custom environments:    https://{environment}.engine.energytransitionmodel.com/api/v3
        (e.g., 'tyndp2024' -> https://tyndp2024.engine.energytransitionmodel.com/api/v3)

    Falls back to the 'pro' URL if the input is empty.
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

    # Custom environments (e.g., tyndp2024, tyndp2026, etc.)
    # Use the environment string as a subdomain
    return f"https://{env}.engine.energytransitionmodel.com/api/v3"
