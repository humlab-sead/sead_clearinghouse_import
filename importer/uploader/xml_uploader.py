import io
import os
from typing import Any

from loguru import logger
from psycopg import Connection

from importer.utility import log_decorator

from . import BaseUploader, Uploaders

# NFIXME: Deprecate XML uploader in favor of CSV uploader.


@Uploaders.register(key="xml")
class XmlUploader(BaseUploader):
    """Upload submission file to database using legacy XML files."""

    def __init__(self, *, target_schema: str = "clearing_house") -> None:
        self.target_schema: str = target_schema

    @log_decorator(enter_message=" ---> uploading XML...", exit_message=" ---> XML uploaded", level="DEBUG")
    def upload(self, connection: Connection, source: str | Any, submission_id: int) -> None:
        """Upload processed XML submission file to database."""
        if source is None:
            raise ValueError("Either xml or filename must be provided")

        if not isinstance(source, str):
            raise ValueError("XML must be a string or a filename")

        if "<" in source:
            xml: str = source
        else:
            if not os.path.exists(source):
                raise ValueError(f"XML file {source} does not exist")
            with io.open(source, mode="r", encoding="utf-8") as f:
                xml: str = f.read()

        with connection.cursor() as cursor:
            sql = """ update clearing_house.tbl_clearinghouse_submissions set xml = %s where submission_id = %s; """
            cursor.execute(sql, (xml, submission_id))
        connection.commit()

    @log_decorator(
        enter_message=" ---> extracting submission...", exit_message=" ---> submission extracted", level="DEBUG"
    )
    def extract(self, connection: Connection, submission_id: int) -> None:
        """Extract submission into staging tables."""
        with connection.cursor() as cursor:
            logger.info("   --> extracting table names from xml...")
            cursor.callproc("clearing_house.fn_extract_and_store_submission_tables", (submission_id,))  # type: ignore

            logger.info("   --> extracting columns from xml...")
            cursor.callproc("clearing_house.fn_extract_and_store_submission_columns", (submission_id,))  # type: ignore

            logger.info("   --> extracting records from xml...")
            cursor.callproc("clearing_house.fn_extract_and_store_submission_records", (submission_id,))  # type: ignore

            logger.info("   --> extracting values from xml...")
            cursor.callproc("clearing_house.fn_extract_and_store_submission_values", (submission_id,))  # type: ignore
