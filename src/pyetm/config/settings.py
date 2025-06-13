from pathlib import Path
import yaml
from typing import Optional, ClassVar
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

class AppConfig(BaseSettings):
    """
    Application configuration loaded from YAML.
    """
    etm_api_token: str = Field(..., description="API token for ETM")
    base_url: str = Field(
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

# Locate `config.yml` at project root.
PROJECT_ROOT = Path(__file__).parents[3]
CONFIG_FILE = PROJECT_ROOT / "config.yml"

# Singleton instance of AppConfig
settings = AppConfig.from_yaml(CONFIG_FILE)
