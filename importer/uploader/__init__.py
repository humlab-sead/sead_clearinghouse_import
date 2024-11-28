import abc
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
