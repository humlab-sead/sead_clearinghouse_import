import abc
import importlib
import os
from typing import Any

from loguru import logger
from psycopg2.extensions import connection as Connection

from importer.utility import Registry


class BaseUploader(abc.ABC):
    @abc.abstractmethod
    def upload(self, connection: Connection, xml_filename: str | Any, submission_id: int) -> None:
        pass

    @abc.abstractmethod
    def extract(self, connection: Connection, submission_id: int) -> None:
        pass


class UploaderRegistry(Registry):
    items: dict = {}


Uploaders: UploaderRegistry = UploaderRegistry()


__all__ = []
current_dir: str = os.path.dirname(__file__)
for filename in os.listdir(current_dir):
    if filename.endswith(".py") and filename != "__init__.py":
        module_name: str = filename[:-3]
        __all__.append(module_name)
        importlib.import_module(f".{module_name}", package=__name__)
