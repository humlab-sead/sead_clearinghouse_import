import io
import os
from typing import Any

from loguru import logger
from psycopg2.extensions import connection as Connection

from ..utility import log_decorator
from . import BaseUploader, Uploaders


@Uploaders.register(key="xml")
class XmlUploader(BaseUploader):
    """Upload submission file to database using legacy XML files."""

    def __init__(self, *, target_schema: str = "clearing_house") -> None:
        self.target_schema: str = target_schema

    @log_decorator(enter_message=" ---> uploading XML...", exit_message=" ---> XML uploaded")
    def upload(self, connection: Connection, xml_filename: str | Any, submission_id: int) -> None:
        """Upload processed XML submission file to database."""
        if xml_filename is None:
            raise ValueError("Either xml or filename must be provided")

        if not isinstance(xml_filename, str):
            raise ValueError("XML must be a string or a filename")

        if '<' in xml_filename:
            xml: str = xml_filename
        else:
            if not os.path.exists(xml_filename):
                raise ValueError(f"XML file {xml_filename} does not exist")
            with io.open(xml_filename, mode="r", encoding="utf-8") as f:
                xml: str = f.read()

        with connection.cursor() as cursor:
            sql = """ update clearing_house.tbl_clearinghouse_submissions set xml = %s where submission_id = %s; """
            cursor.execute(sql, (xml, submission_id))
        connection.commit()

    @log_decorator(enter_message=" ---> extracting submission...", exit_message=" ---> submission extracted")
    def extract(self, connection: Connection, submission_id: int) -> None:
        """Extract submission into staging tables."""
        with connection.cursor() as cursor:
            logger.info("   --> extracting table names from xml...")
            cursor.callproc("clearing_house.fn_extract_and_store_submission_tables", (submission_id,))

            logger.info("   --> extracting columns from xml...")
            cursor.callproc("clearing_house.fn_extract_and_store_submission_columns", (submission_id,))

            logger.info("   --> extracting records from xml...")
            cursor.callproc("clearing_house.fn_extract_and_store_submission_records", (submission_id,))

            logger.info("   --> extracting values from xml...")
            cursor.callproc("clearing_house.fn_extract_and_store_submission_values", (submission_id,))
