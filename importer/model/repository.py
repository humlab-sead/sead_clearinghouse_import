import contextlib
from typing import Any

import psycopg2
from loguru import logger
from psycopg2 import extensions as pgext


class SubmissionRepository:
    def __init__(self, db_opts: dict[str, str]) -> None:
        self.db_opts: dict[str, str] = db_opts
        self.connection: pgext.connection = None

    def __enter__(self) -> pgext.connection:
        return self.open()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def open(self) -> pgext.connection:
        if self.connection is None:
            self.connection = psycopg2.connect(**self.db_opts)
        return self.connection

    def close(self) -> None:
        if self.connection is not None:
            with contextlib.suppress(Exception):
                self.connection.close()
        self.connection = None

    def commit(self) -> None:
        if self.connection is not None:
            try:
                self.connection.commit()
            except Exception as _:  # pylint: disable=W0703
                logger.exception("commit failed")

    def execute(self, proc_name: str, args: tuple) -> None:
        with self.open() as cursor:
            cursor.callproc(proc_name, args)

    def get_table_names(self, submission_id: int) -> list[str]:
        tables_names_sql: str = """
            Select Distinct t.table_name_underscored
            From clearing_house.tbl_clearinghouse_submission_tables t
            Join clearing_house.tbl_clearinghouse_submission_xml_content_tables c
                On c.table_id = t.table_id
            Where c.submission_id = %s
        """
        with self.open().cursor() as cursor:
            cursor.execute(tables_names_sql, (submission_id,))
            table_names: list[tuple[Any, ...]] = cursor.fetchall()
        return table_names

    def extract_to_staging_tables(self, submission_id: int) -> None:
        """Extract submission into staging tables."""
        with self.open().cursor() as cursor:
            logger.info("   --> extracting table names from xml...")
            cursor.callproc(
                "clearing_house.fn_extract_and_store_submission_tables",
                (submission_id,),
            )

            logger.info("   --> extracting columns from xml...")
            cursor.callproc(
                "clearing_house.fn_extract_and_store_submission_columns",
                (submission_id,),
            )

            logger.info("   --> extracting records from xml...")
            cursor.callproc(
                "clearing_house.fn_extract_and_store_submission_records",
                (submission_id,),
            )

            logger.info("   --> extracting values from xml...")
            cursor.callproc(
                "clearing_house.fn_extract_and_store_submission_values",
                (submission_id,),
            )

            logger.info("   --> extraction done!")
            self.commit()

    def explode_to_public_tables(
        self,
        submission_id: int,
        p_dry_run: bool = False,
        p_add_missing_columns: bool = False,
    ) -> None:
        """Explode submission into public tables."""
        with self.open().cursor() as cursor:
            for table_name_underscored in self.get_table_names(submission_id):
                logger.info("   --> Processing table %s", table_name_underscored)
                if p_add_missing_columns:
                    cursor.callproc(
                        "clearing_house.fn_add_new_public_db_columns",
                        (submission_id, table_name_underscored),
                    )
                if not p_dry_run:
                    cursor.callproc(
                        "clearing_house.fn_copy_extracted_values_to_entity_table",
                        (submission_id, table_name_underscored),
                    )
        self.commit()

    def remove(
        self,
        submission_id: int,
        clear_header: bool = False,
        clear_exploded: bool = True,
    ) -> None:
        """Delete submission from staging tables."""
        logger.info("   --> Cleaning up existing data for submission...")
        with self.open().cursor() as cursor:
            cursor.callproc(
                "clearing_house.fn_delete_submission",
                (submission_id, clear_header, clear_exploded),
            )
            self.commit()

    def set_pending(self, submission_id: int) -> None:
        with self.open().cursor() as cursor:
            sql: str = """
                update clearing_house.tbl_clearinghouse_submissions
                    set submission_state_id = %s, status_text = %s
                where submission_id = %s
            """
            cursor.execute(sql, (2, "Pending", submission_id))
            self.commit()

    def register(self, xml: str, data_types: str = "") -> int:
        with self.open().cursor() as cursor:
            sql = """
                insert into clearing_house.tbl_clearinghouse_submissions(submission_state_id, data_types, upload_user_id, xml, status_text)
                values (%s, %s, %s, %s, %s) returning submission_id;
            """
            cursor.execute(sql, (1, data_types, 4, xml, "New"))
            submission_id: int = cursor.fetchone()[0]
            self.commit()
        return submission_id
