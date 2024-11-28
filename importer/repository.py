from typing import Any

import psycopg2
from loguru import logger
from psycopg2.extensions import connection as Connection

from .uploader.xml_uploader import BaseUploader, Uploaders
from .utility import log_decorator


class SubmissionRepository:
    def __init__(self, db_options: dict[str, str], uploader: str | BaseUploader = None) -> None:
        self.db_options: dict[str, str] = db_options
        self.uploader: BaseUploader | None = uploader if uploader is BaseUploader else Uploaders.get(uploader)()
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
