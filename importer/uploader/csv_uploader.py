from typing import Any

from psycopg2.extensions import connection as Connection

from ..utility import log_decorator
from . import BaseUploader, Uploaders
from .xml_to_csv import xml_to_csv_to_db


@Uploaders.register(key="csv")
class XmlToCsvUploader(BaseUploader):
    """Upload submission file to database using CSV files."""

    def __init__(self, *, csv_folder: str = "./csv_files", target_schema: str = "clearing_house") -> None:
        self.csv_folder: str = csv_folder
        self.target_schema: str = target_schema

    @log_decorator(enter_message=" ---> uploading CSV submission...", exit_message=" ---> CSV submission uploaded", level="DEBUG")
    def upload(
        self,
        connection: Connection,
        xml_filename: str | Any,
        submission_id: int,  # pylint: disable=unused-argument
    ) -> None:
        xml_to_csv_to_db(connection, xml_filename, self.csv_folder, self.target_schema)

    @log_decorator(enter_message=" ---> extracting submission...", exit_message=" ---> submission extracted", level="DEBUG")
    def extract(self, connection: Connection, submission_id: int) -> None:
        """Extract submission into staging tables."""
        with connection.cursor() as cursor:
            cursor.callproc("clearing_house.fn_extract_csv_upload_to_staging_tables", (submission_id,))
