import io
import time
from dataclasses import dataclass, field
from os.path import basename, join, splitext
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger

from importer.model import (
    DataImportError,
    Metadata,
    SpecificationError,
    SubmissionData,
    SubmissionRepository,
    SubmissionSpecification,
    TableSpec,
)

from . import to_xml, utility

# pylint: disable=too-many-instance-attributes


@dataclass
class Options:
    """
    Options for the importer
    """

    dbname: str
    dbuser: str
    dbhost: str
    port: int
    output_folder: str
    filename: str
    table_names: str
    skip: bool
    submission_id: int
    xml_filename: str
    data_types: str
    check_only: bool
    register: bool
    explode: bool
    timestamp: bool
    tidy_xml: bool
    log_folder: str = field(default="./logs")
    transfer_format: str = field(default="xml")
    ignore_columns: list[str] = None
    basename: str = field(init=False, default=None)
    target: str = field(init=False, default=None)

    def __post_init__(self) -> None:
        self.basename: str = splitext(basename(self.filename))[0]
        self.target: str = (
            join(self.output_folder, f"{self.basename}_{time.strftime('%Y%m%d-%H%M%S')}.xml")
            if self.timestamp
            else join(self.output_folder, f"{self.basename}.xml")
        )
        self.ignore_columns: list[str] = self.ignore_columns if self.ignore_columns is not None else ["date_updated"]

    def db_uri(self) -> str:
        return "postgresql://{}@{}:{}/{}".format(self.dbuser, self.dbhost, self.port, self.dbname)

    @property
    def db_opts(self) -> dict[str, Any]:
        # assert (
        #     os.environ.get("SEAD_CH_PASSWORD", None) is not None
        # ), "fatal: environment variable SEAD_CH_PASSWORD not set!"
        return dict(
            database=self.dbname,
            user=self.dbuser,
            # password=os.environ["SEAD_CH_PASSWORD"],
            host=self.dbhost,
            port=self.port,
        )


class ImportService:
    def __init__(
        self,
        *,
        opts: Options,
        metadata: Metadata = None,
        repository: SubmissionRepository = None,
        xml_processor: to_xml.XmlProcessor = None,
    ) -> None:
        self.opts: Options = opts
        self.repository: SubmissionRepository = repository or SubmissionRepository(
            opts.db_opts, uploader=opts.transfer_format
        )
        self.metadata: Metadata = metadata or Metadata(opts.db_uri())
        self.xml_processor: to_xml.XmlProcessor = xml_processor or to_xml.XmlProcessor
        self.specification: SubmissionSpecification = SubmissionSpecification(
            metadata=self.metadata, ignore_columns=self.opts.ignore_columns, raise_errors=False
        )

    @utility.log_decorator(enter_message=" ---> generating XML file...", exit_message=" ---> XML created")
    def to_xml(self, submission: SubmissionData, format_document: bool = False) -> str:
        """
        Reads Excel files and convert content to an CH XML-file.
        Stores submission in output_filename and returns filename for a cleaned up version of the XML
        """

        update_missing_system_id_to_public_id(self.metadata, submission)

        with io.open(self.opts.target, "w", encoding="utf8") as outstream:
            self.xml_processor(outstream).process(self.metadata, submission, self.opts.table_names)

        if format_document:
            self.opts.target: str = utility.tidy_xml(self.opts.target, remove_source=True)

        logger.info(f" ---> XML file created: {self.opts.target}")

        return self.opts.target

    @utility.log_decorator(enter_message="Processing started...", exit_message="Processing done")
    def process(self, submission: str | SubmissionData) -> None:
        try:
            opts: Options = self.opts
            if opts.skip is True:
                logger.info("Skipping: %s", opts.basename)
                return

            if isinstance(submission, SubmissionData):
                if not self.specification.is_satisfied_by(submission):
                    logger.error(f" ---> {opts.basename} does not satisfy the specification")
                    return
                if self.opts.check_only:
                    logger.info(f" ---> {opts.basename} satisfies the specification")
                    return

            if (opts.submission_id or 0) > 0:
                self.repository.remove(opts.submission_id, clear_header=False, clear_exploded=False)

            if (opts.submission_id or 0) == 0:
                opts.xml_filename = (
                    submission
                    if isinstance(submission, str)
                    else self.to_xml(submission, format_document=opts.tidy_xml)
                )

                if opts.register:
                    opts.submission_id = self.repository.register(data_types=opts.data_types)

                    self.repository.upload_xml(opts.xml_filename, opts.submission_id)
                    self.repository.extract_to_staging_tables(opts.submission_id)

            if opts.explode is True:
                self.repository.explode_to_public_tables(
                    opts.submission_id, p_dry_run=False, p_add_missing_columns=False
                )
                self.repository.set_pending(opts.submission_id)

        except SpecificationError:
            logger.exception(f"aborted critical error {opts.basename}")


def update_missing_system_id_to_public_id(metadata: Metadata, submission: SubmissionData) -> None:
    """For each table in index, update system_id to public_id if isnan. This should be avoided though."""
    for table_name in submission.index_table_names:
        try:
            data_table: pd.DataFrame = submission.data_tables[table_name]
            table_spec: TableSpec = metadata[table_name]

            pk_name: str = table_spec.pk_name

            if pk_name == "ceramics_id":
                pk_name = "ceramic_id"

            if data_table is None or pk_name not in data_table.columns:
                continue

            if "system_id" not in data_table.columns:
                raise DataImportError(f'critical error Table {table_name} has no column named "system_id"')

            # Update system_id to public_id if isnan. This should be avoided though.
            data_table.loc[np.isnan(data_table.system_id), "system_id"] = data_table.loc[
                np.isnan(data_table.system_id), pk_name
            ]

        except DataImportError as _:
            logger.error(f"error {table_name} when updating system_id ")
            logger.exception("update_system_id")
            continue
