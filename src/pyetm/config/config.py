import os
from pathlib import Path
from dotenv import load_dotenv

#TODO: expand when full config is established
load_dotenv(Path(__file__).parent.parent / ".env")

from ..utils.singleton import SingletonMeta

class Config(metaclass=SingletonMeta):
    """
    Singleton config loader reading from environment variables.
    Raises if ETM_API_TOKEN is not set.
    """
    def __init__(self):
        token = os.getenv("ETM_API_TOKEN")
        if not token:
            raise RuntimeError("ETM_API_TOKEN environment variable is required")
        self.etm_api_token = token
        self.base_url = os.getenv(
            "BASE_URL",
            "https://engine.energytransitionmodel.com/api/v3"
        )

config = Config()
