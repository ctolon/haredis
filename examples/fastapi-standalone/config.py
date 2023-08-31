from pathlib import Path
import logging
import os

import dotenv
from uvicorn.config import LOGGING_CONFIG

# Meta Settings
class MetaSettings(object):
    PROJECT_NAME: str = "haredis"

# Project Root Directory
PARENT_DIR = Path(__file__).parent.resolve()

# Parse Config.ini file with configparser and provided path
FP_CONFIG = Path(Path(__file__).parent, ".env")
settings = dotenv.load_dotenv(FP_CONFIG)

# Parse Redis DB settings from config.ini file
class RedisSettings(object):
    HOST: str = os.getenv("REDIS_HOST", "redis")
    PORT: int = int(os.getenv("REDIS_PORT", 6379))
    DB: str = os.getenv("REDIS_DB", 0)
    PASSWORD: str = os.getenv("REDIS_PASSWORD", "examplepass")
    

# Parser Microservice API Settings from config.ini file
class APISettings(object):

    HOST: str = os.getenv("API_HOST", "0.0.0.0")
    PORT: int = int(os.getenv("API_PORT", 8000))
    

LOGGING_CONFIG["formatters"]["default"]["fmt"] = "%(asctime)s [%(name)s] %(levelprefix)s %(message)s"
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


if __name__ == "__main__":
    
    # Print Configs for Debugging.
    print(FP_CONFIG)
    print(PARENT_DIR)
