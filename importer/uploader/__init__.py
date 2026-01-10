import abc
import importlib
import os
from typing import Any

from loguru import logger
from psycopg import Connection

from importer.utility import Registry


class BaseUploader(abc.ABC):
    @abc.abstractmethod
    def upload(self, connection: Connection, xml_filename: str | Any, submission_id: int) -> None:
        pass

    @abc.abstractmethod
    def extract(self, connection: Connection, submission_id: int) -> None:
        pass

class NullUploader(BaseUploader):
    def upload(self, connection: Connection, xml_filename: str | Any, submission_id: int) -> None:  # pylint: disable=unused-argument
        raise ValueError("No uploader specified")

    def extract(self, connection: Connection, submission_id: int) -> None:  # pylint: disable=unused-argument
        raise ValueError("No uploader specified")
    
class UnknownUploader(BaseUploader):

    def upload(self, connection: Connection, xml_filename: str | Any, submission_id: int) -> None:  # pylint: disable=unused-argument
        raise ValueError("Invalid uploader specified")
    
    def extract(self, connection: Connection, submission_id: int) -> None:  # pylint: disable=unused-argument
        raise ValueError("Invalid uploader specified")
    
class UploaderRegistry(Registry):
    items: dict = {}

    def get(self, key: str, default: Any = None) -> Any:
        return self.items.get(key, UnknownUploader)
    
Uploaders: UploaderRegistry = UploaderRegistry()  # pylint: disable=invalid-name


__all__ = []
current_dir: str = os.path.dirname(__file__)
for filename in os.listdir(current_dir):
    if filename.endswith(".py") and filename != "__init__.py":
        module_name: str = filename[:-3]
        __all__.append(module_name)
        importlib.import_module(f".{module_name}", package=__name__)
