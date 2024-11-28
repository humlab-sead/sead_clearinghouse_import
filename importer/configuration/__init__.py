# type: ignore
import os

import dotenv

from .config import Config
from .inject import ConfigStore, ConfigValue, configure_context, inject_config


def dburi_from_env() -> str:
    """
    Returns the database URI from the environment variables.
    """
    dotenv.load_dotenv(".env")
    return f"postgresql://{os.environ['DBUSER']}@{os.environ['DBHOST']}:{os.environ.get('DBPORT', '5432')}/{os.environ['DBNAME']}"
