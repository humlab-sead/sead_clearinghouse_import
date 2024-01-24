import abc
import io
import os
from typing import Any, Type

import psycopg2
from loguru import logger
from psycopg2.extensions import connection as Connection

from .to_csv import xml_to_csv_to_db
from .utility import log_decorator


class BaseUploader(abc.ABC):
    @abc.abstractmethod
    def upload(self, connection: Connection, xml_filename: str | Any, submission_id: int) -> None:
        pass

    @abc.abstractmethod
    def extract(self, connection: Connection, submission_id: int) -> None:
        pass


class XmlUploader(BaseUploader):
    def __init__(self, *, target_schema: str = "clearing_house") -> None:
        self.target_schema: str = target_schema

    @log_decorator(enter_message=" ---> uploading XML...", exit_message=" ---> XML uploaded")
    def upload(self, connection: Connection, xml_filename: str | Any, submission_id: int) -> None:
        if xml_filename is None:
            raise ValueError("Either xml or filename must be provided")

        if not isinstance(xml_filename, str):
            raise ValueError("XML must be a string or a filename")

        if len(xml_filename) < 200:
            if not os.path.exists(xml_filename):
                raise ValueError("XML file does not exist")
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


class CsvUploader(BaseUploader):
    def __init__(self, *, csv_folder: str = "./csv_files", target_schema: str = "clearing_house") -> None:
        self.csv_folder: str = csv_folder
        self.target_schema: str = target_schema

    @log_decorator(enter_message=" ---> uploading CSV submission...", exit_message=" ---> CSV submission uploaded")
    def upload(
        self,
        connection: Connection,
        xml_filename: str | Any,
        submission_id: int,  # pylint: disable=unused-argument
    ) -> None:
        xml_to_csv_to_db(connection, xml_filename, self.csv_folder, self.target_schema)

    @log_decorator(enter_message=" ---> extracting submission...", exit_message=" ---> submission extracted")
    def extract(self, connection: Connection, submission_id: int) -> None:
        """Extract submission into staging tables."""
        with connection.cursor() as cursor:
            cursor.callproc("clearing_house.fn_extract_csv_upload_to_staging_tables", (submission_id,))


UPLOADERS: dict[str, Type[BaseUploader]] = {"xml": XmlUploader, "csv": CsvUploader}


class SubmissionRepository:
    def __init__(self, db_options: dict[str, str], uploader: BaseUploader = None) -> None:
        self.db_options: dict[str, str] = db_options
        self.uploader: BaseUploader | None = (
            uploader if uploader is BaseUploader else UPLOADERS.get(uploader, XmlUploader)()
        )
        self.connection: Connection | None = None
        self.timeout_seconds: int = 300

    def upload_xml(self, xml_filename: str, submission_id: int) -> None:
        with self as connection:
            self.uploader.upload(connection, xml_filename, submission_id)

    @log_decorator(enter_message=" ---> extracting submission...", exit_message=" ---> submission extracted")
    def extract_to_staging_tables(self, submission_id: int) -> None:
        with self as connection:
            self.uploader.extract(connection, submission_id)

    @log_decorator(enter_message=" ---> exploding submission...", exit_message=" ---> submission exploded")
    def explode_to_public_tables(
        self,
        submission_id: int,
        p_dry_run: bool = False,
        p_add_missing_columns: bool = False,
    ) -> None:
        """Explode submission into public tables."""
        with self as connection:
            for table_name_underscored in self.get_table_names(submission_id):
                logger.info(f"   --> Processing table {table_name_underscored}")
                if p_add_missing_columns:
                    with connection.cursor() as cursor:
                        cursor.callproc(
                            "clearing_house.fn_add_new_public_db_columns", (submission_id, table_name_underscored)
                        )
                if not p_dry_run:
                    with connection.cursor() as cursor:
                        cursor.callproc(
                            "clearing_house.fn_copy_extracted_values_to_entity_table",
                            (submission_id, table_name_underscored),
                        )

    @log_decorator(enter_message=" ---> removing submission...", exit_message=" ---> submission removed")
    def remove(self, submission_id: int, clear_header: bool = False, clear_exploded: bool = True) -> None:
        """Delete submission from staging tables."""
        logger.info("   --> Cleaning up existing data for submission...")
        with self as connection:
            with connection.cursor() as cursor:
                cursor.callproc(
                    "clearing_house.fn_delete_submission",
                    (submission_id, clear_header, clear_exploded),
                )

    @log_decorator(enter_message=" ---> setting state to pending...", exit_message=" ---> state set to pending")
    def set_pending(self, submission_id: int) -> None:
        with self as connection:
            with connection.cursor() as cursor:
                sql: str = """
                    update clearing_house.tbl_clearinghouse_submissions
                        set submission_state_id = %s, status_text = %s
                    where submission_id = %s
                """
                cursor.execute(sql, (2, "Pending", submission_id))

    @log_decorator(enter_message=" ---> registering submission...", exit_message=" ---> submission registered")
    def register(self, *, data_types: str = "") -> int:
        # if xml is None and filename is None:
        #     raise ValueError("Either xml or filename must be provided")

        # if xml is None:
        #     with io.open(filename, mode="r", encoding="utf-8") as f:
        #         xml: str = f.read()
        with self as connection:
            with connection.cursor() as cursor:
                sql = """
                    insert into clearing_house.tbl_clearinghouse_submissions(submission_state_id, data_types, upload_user_id, xml, status_text)
                    values (%s, %s, %s, NULL, %s) returning submission_id;
                """
                cursor.execute(sql, (1, data_types, 4, "New"))
                submission_id: int = cursor.fetchone()[0]
            return submission_id

    def get_table_names(self, submission_id: int) -> list[str]:
        tables_names_sql: str = """
            select distinct t.table_name_underscored
            from clearing_house.tbl_clearinghouse_submission_tables t
            join clearing_house.tbl_clearinghouse_submission_xml_content_tables c
                on c.table_id = t.table_id
            where c.submission_id = %s
        """
        with self.connection.cursor() as cursor:
            cursor.execute(tables_names_sql, (submission_id,))
            table_names: list[tuple[Any, ...]] = cursor.fetchall()
        return table_names

    # def table_exists(self, table_schema: str, table_name: str) -> bool:
    #     with self.connection.cursor() as cursor:
    #         cursor.execute(
    #             """
    #             select exists (
    #                 select from information_schema.tables
    #                 where  table_schema = %s
    #                 and  table_name = %s
    #             )
    #         """,
    #             (table_schema, table_name),
    #         )
    #         return cursor.fetchone()[0]

    # def execute(self, proc_name: str, args: tuple) -> None:
    #     with self.connection.cursor() as cursor:
    #         cursor.callproc(proc_name, args)

    def __enter__(self) -> Connection:
        if self.connection is None:
            timeout_ms: int = self.timeout_seconds * 1000
            self.connection: Connection = psycopg2.connect(
                **self.db_options,
                options=f'-c statement_timeout={timeout_ms} -c idle_in_transaction_session_timeout={timeout_ms}',
            )
        return self.connection

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connection:
            if exc_type is not None:
                self.connection.rollback()
            else:
                self.connection.commit()
            self.connection.close()
        self.connection = None
