import io

# -*- coding: utf-8 -*-
import os
import time
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger

from importer.model import DataImportError, Metadata, SubmissionData, SubmissionRepository

from . import process_xml, utility


class ImportService:
    @dataclass
    class Options:
        """
        Options for the importer
        """

        dbname: str
        dbuser: str
        dbhost: str
        port: int
        input_folder: str
        output_folder: str
        data_filename: str
        table_names: str
        skip: bool
        submission_id: int
        xml_filename: str
        data_types: str

        def db_uri(self) -> str:
            return "postgresql://{}@{}:{}/{}".format(self.dbuser, self.dbhost, self.port, self.dbname)

    def __init__(self, opts: Options) -> None:
        self.opts: ImportService.Options = opts
        assert (
            os.environ.get("SEAD_CH_PASSWORD", None) is not None
        ), "fatal: environment variable SEAD_CH_PASSWORD not set!"
        db_opts: dict[str] = dict(
            database=opts.dbname,
            user=opts.dbuser,
            password=os.environ["SEAD_CH_PASSWORD"],
            host=opts.dbhost,
            port=opts.port,
        )
        self.repository = SubmissionRepository(db_opts)

    def process_excel_to_xml(self, option: Options, basename: str, timestamp: str) -> str:
        """
        Reads Excel files and convert content to an CH XML-file.
        Stores submission in output_filename and returns filename for a cleaned up version of the XML
        """
        data_filename: str = os.path.join(option.input_folder, option.data_filename)
        output_filename: str = os.path.join(option.output_folder, "{}_{}.xml".format(basename, timestamp))

        metadata: Metadata = Metadata(option.db_uri())

        submission: SubmissionData = SubmissionData().load(metadata, data_filename)

        update_missing_system_id_to_public_id(metadata, submission)

        with io.open(output_filename, "w", encoding="utf8") as outstream:
            process_xml.XmlProcessor(outstream).process(metadata, submission, option.table_names)

        tidy_output_filename: str = utility.tidy_xml(output_filename)

        if tidy_output_filename != output_filename:
            os.remove(output_filename)

        return tidy_output_filename

    def upload_xml(self, xml_filename: str, data_types: str = "") -> int:
        with io.open(xml_filename, mode="r", encoding="utf-8") as f:
            xml: str = f.read()

        submission_id: int = self.repository.register(xml, data_types=data_types)

        return submission_id

    def process(self) -> None:
        try:
            basename: str = os.path.splitext(self.opts.data_filename)[0]

            if self.opts.skip is True:
                logger.info("Skipping: %s", basename)
                return

            timestamp: str = time.strftime("%Y%m%d-%H%M%S")

            logger.info("PROCESS OF %s STARTED", basename)

            if (self.opts.submission_id or 0) == 0:
                if self.opts.xml_filename is not None:
                    logger.info(" ---> UPLOADING EXISTING FILE {}".format(self.opts.xml_filename))
                else:
                    logger.info(" ---> PARSING EXCEL EXCEL")
                    self.opts.xml_filename = self.process_excel_to_xml(self.opts, basename, timestamp)

                logger.info(" ---> UPLOAD STARTED!")
                self.opts.submission_id = self.upload_xml(self.opts.xml_filename, data_types=self.opts.data_types)
                logger.info(" ---> UPLOAD DONE ID=%s", self.opts.submission_id)

                logger.info(" ---> EXTRACT STARTED!")
                self.repository.extract_to_staging_tables(self.opts.submission_id)
                logger.info(" ---> EXTRACT DONE")

            else:
                self.repository.remove(self.opts.submission_id, clear_header=False, clear_exploded=False)
                logger.info(" ---> USING EXISTING DATA ID=%s", self.opts.submission_id)

            logger.info(" ---> EXPLODE STARTED")
            self.repository.explode_to_public_tables(
                self.opts.submission_id, p_dry_run=False, p_add_missing_columns=False
            )
            logger.info(" ---> EXPLODE DONE")

            self.repository.set_pending(self.opts.submission_id)
            logger.info(" ---> PROCESS OF %s DONE", basename)

        except:  # pylint: disable=bare-except
            logger.exception("ABORTED CRITICAL ERROR %s ", basename)


def update_missing_system_id_to_public_id(metadata: Metadata, submission: SubmissionData) -> None:
    """For each table in index, update system_id to public_id if isnan. This should be avoided though."""
    for table_name in submission.index_tablenames:
        try:
            data_table: pd.DataFrame = submission.data_tables[table_name]
            table_definition: dict[str, Any] = metadata[table_name]

            pk_name: str = table_definition["pk_name"]

            if pk_name == "ceramics_id":
                pk_name = "ceramic_id"

            if data_table is None or pk_name not in data_table.columns:
                continue

            if "system_id" not in data_table.columns:
                raise DataImportError('CRITICAL ERROR Table {} has no column named "system_id"'.format(table_name))

            # Update system_id to public_id if isnan. This should be avoided though.
            data_table.loc[np.isnan(data_table.system_id), "system_id"] = data_table.loc[
                np.isnan(data_table.system_id), pk_name
            ]

        except DataImportError as _:
            logger.error("ERROR {} when updating system_id ".format(table_name))
            logger.exception("update_system_id")
            continue
