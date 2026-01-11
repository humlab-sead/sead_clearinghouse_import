import os
from typing import Any

import pandas as pd
from psycopg import Connection
from sqlalchemy import TEXT

from importer.uploader.xml_to_csv import xml_to_csv_to_db
from importer.utility import get_connection_uri, log_decorator

from . import BaseUploader, Uploaders


@Uploaders.register(key="xml_to_csv")
class XmlToCsvUploader(BaseUploader):
    """Upload submission file to database using CSV files."""

    def __init__(self, *, source: str = "./csv_files", target_schema: str = "clearing_house") -> None:
        self.source: str = source
        self.target_schema: str = target_schema
        self.csv_folder: str = os.path.dirname(self.source)

    @log_decorator(
        enter_message=" ---> uploading CSV submission...", exit_message=" ---> CSV submission uploaded", level="DEBUG"
    )
    def upload(
        self,
        connection: Connection,
        source: str | Any,
        submission_id: int,  # pylint: disable=unused-argument
    ) -> None:
        xml_to_csv_to_db(connection, source, self.csv_folder, self.target_schema)

    @log_decorator(
        enter_message=" ---> extracting submission...", exit_message=" ---> submission extracted", level="DEBUG"
    )
    def extract(self, connection: Connection, submission_id: int) -> None:
        """Extract submission into staging tables."""
        with connection.cursor() as cursor:
            cursor.callproc("clearing_house.fn_extract_csv_upload_to_staging_tables", (submission_id,))  # type: ignore
