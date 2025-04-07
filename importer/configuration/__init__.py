# type: ignore
import os

import dotenv

from .config import Config
from .inject import ConfigStore, ConfigValue, configure_context, inject_config
