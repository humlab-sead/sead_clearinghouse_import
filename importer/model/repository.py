# -*- coding: utf-8 -*-
import logging
import psycopg2

from importer import utility

logger = logging.getLogger('Excel XML processor')
utility.setup_logger(logger)

class SubmissionRepository():

    def __init__(self, db_opts):
        utility.setup_logger(logger)

        self.db_opts = db_opts
        self.connection = None

    def __enter__(self):
        return self.open()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def open(self):
        if self.connection is None:
            self.connection = psycopg2.connect(**self.db_opts)
        return self.connection

    def close(self):
        if self.connection is not None:
            try: self.connection.close()
            except: pass
        self.connection = None

    def commit(self):
        if self.connection is not None:
            try:
                self.connection.commit()
            except Exception as _: # pylint: disable=W0703
                logger.exception('commit failed')

    def execute(self, proc_name, args):
        with self.open() as cursor:
            cursor.callproc(proc_name, args)

    def get_table_names(self, submission_id):
        tables_names_sql = """
            Select Distinct t.table_name_underscored
            From clearing_house.tbl_clearinghouse_submission_tables t
            Join clearing_house.tbl_clearinghouse_submission_xml_content_tables c
                On c.table_id = t.table_id
            Where c.submission_id = %s
        """
        with self.open().cursor() as cursor:
            cursor.execute(tables_names_sql, (submission_id,))
            table_names = cursor.fetchall()
        return table_names

    def extract_submission(self, submission_id):
        with self.open().cursor() as cursor:
            logger.info('   --> Extracting table names from XML...')
            cursor.callproc('clearing_house.fn_extract_and_store_submission_tables', (submission_id,))
            logger.info('   --> Extracting columns from XML...')
            cursor.callproc('clearing_house.fn_extract_and_store_submission_columns', (submission_id,))
            logger.info('   --> Extracting records from XML...')
            cursor.callproc('clearing_house.fn_extract_and_store_submission_records', (submission_id,))
            logger.info('   --> Extracting values from XML...')
            cursor.callproc('clearing_house.fn_extract_and_store_submission_values', (submission_id,))
            logger.info('   --> Extraction done!')
            self.commit()

    def explode_submission(self, submission_id, p_dry_run=False, p_add_missing_columns=False):
        with self.open().cursor() as cursor:
            for table_name_underscored in self.get_table_names(submission_id):
                logger.info('   --> Processing table %s', table_name_underscored)
                if p_add_missing_columns:
                    cursor.callproc('clearing_house.fn_add_new_public_db_columns', (submission_id, table_name_underscored))
                if not p_dry_run:
                    cursor.callproc('clearing_house.fn_copy_extracted_values_to_entity_table', (submission_id, table_name_underscored))
        self.commit()

    def delete_submission(self, submission_id, clear_header=False, clear_exploded=True):
        logger.info('   --> Cleaning up existing data for submission...')
        with self.open().cursor() as cursor:
            cursor.callproc('clearing_house.fn_delete_submission', (submission_id, clear_header, clear_exploded))
            self.commit()

    def set_pending(self, submission_id):
        with self.open().cursor() as cursor:
            update_sql = '''
                UPDATE clearing_house.tbl_clearinghouse_submissions
                    SET submission_state_id = %s, status_text = %s
                WHERE submission_id = %s
            '''
            cursor.execute(update_sql, (2, 'Pending', submission_id))
            self.commit()

    def add_xml(self, xml, data_types=''):
        with self.open().cursor() as cursor:
            insert_sql = """
                INSERT INTO clearing_house.tbl_clearinghouse_submissions(submission_state_id, data_types, upload_user_id, xml, status_text)
                VALUES (%s, %s, %s, %s, %s) RETURNING submission_id;
            """
            cursor.execute(insert_sql, (1, data_types, 4, xml, 'New'))
            submission_id = cursor.fetchone()[0]
            self.commit()
        return submission_id


