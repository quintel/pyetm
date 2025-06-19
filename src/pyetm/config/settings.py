from pathlib import Path
import yaml
from typing import Optional, ClassVar, List
from pydantic import Field, ValidationError, HttpUrl
from pydantic_settings import BaseSettings, SettingsConfigDict

class AppConfig(BaseSettings):
    """
    Application configuration loaded from YAML.
    """
    etm_api_token: str = Field(..., description="Your ETM token is missing—please set $ETM_API_TOKEN or config.yml:etm_api_token")
    base_url: HttpUrl = Field(
        "https://engine.energytransitionmodel.com/api/v3",
        description="Base URL for the ETM API",
    )
    log_level: Optional[str] = Field(
        "INFO",
        description="App logging level",
    )

    model_config: ClassVar[SettingsConfigDict] = SettingsConfigDict(
        env_file=None,
        extra="ignore",
        case_sensitive=False
    )

    @classmethod
    def from_yaml(cls, path: Path) -> "AppConfig":
        """
        Load overrides from a YAML file (if present), then overlay environment variables.
        """
        data = {}
        if path.is_file():
            try:
                data = yaml.safe_load(path.read_text()) or {}
            except yaml.YAMLError:
                data = {}
        return cls(**data)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
CONFIG_FILE  = PROJECT_ROOT / "config.yml"

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
