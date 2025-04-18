import io
import time
from dataclasses import dataclass, field
from os.path import basename, join, splitext
from typing import Type

from loguru import logger

from importer.configuration.inject import ConfigValue

from . import utility
from .dispatchers import IDispatcher, to_xml
from .metadata import Metadata
from .repository import SubmissionRepository
from .specification import SpecificationError, SubmissionSpecification
from .submission import Submission

# pylint: disable=too-many-instance-attributes


@dataclass
class Options:
    """
    Options for the importer
    """

    filename: str
    skip: bool
    submission_id: int
    submission_name: str
    data_types: str
    xml_filename: str = None
    table_names: str = None
    check_only: bool = False
    register: bool = False
    explode: bool = False
    timestamp: bool = True
    tidy_xml: bool = False
    ignore_columns: list[str] = None
    basename: str = field(init=False, default=None)
    target: str = field(init=False, default=None)

    output_folder: str = field(default="data/output")
    database: dict[str, str] = field(default_factory=dict)
    transfer_format: str = field(default="xml")
    dump_to_csv: bool = field(default=False)

    def __post_init__(self) -> None:

        if self.filename:
            self.basename: str = splitext(basename(self.filename))[0]
            self.target: str = (
                join(self.output_folder, f"{self.basename}_{time.strftime('%Y%m%d-%H%M%S')}.xml")
                if self.timestamp
                else join(self.output_folder, f"{self.basename}.xml")
            )
        default_ignore_patterns: list[str] = ConfigValue("options:ignore_columns").resolve() or []
        self.ignore_columns: list[str] = (
            self.ignore_columns if self.ignore_columns is not None else default_ignore_patterns
        )

    def db_uri(self) -> str:
        return utility.create_db_uri(**self.database)

    @property
    def use_existing_submission(self) -> bool:
        return self.submission_id is not None and self.submission_id > 0

    @property
    def source_name(self) -> str:
        """Name of the source file"""
        return basename(self.filename) if self.filename else self.submission_name

    
class ImportService:
    def __init__(
        self,
        *,
        opts: Options,
        metadata: Metadata = None,
        repository: SubmissionRepository = None,
        dispatcher_cls: Type[IDispatcher] = None,
    ) -> None:
        self.opts: Options = opts
        self.repository: SubmissionRepository = repository or SubmissionRepository(
            opts.database, uploader=opts.transfer_format
        )
        self.metadata: Metadata = metadata or Metadata(opts.db_uri())
        self.dispatcher_cls: Type[IDispatcher] = dispatcher_cls or to_xml.XmlProcessor
        self.specification: SubmissionSpecification = SubmissionSpecification(
            metadata=self.metadata, ignore_columns=self.opts.ignore_columns, raise_errors=False
        )

    @utility.log_decorator(
        enter_message=" ---> generating target file(s)...", exit_message=" ---> target file(s) created", level="DEBUG"
    )
    def dispatch(self, submission: Submission, format_document: bool = False) -> str:
        """
        Reads Excel files and convert content to an CH XML-file.
        Stores submission in output_filename and returns filename for a cleaned up version of the XML
        """

        with io.open(self.opts.target, "w", encoding="utf8") as outstream:
            self.dispatcher_cls(outstream).dispatch(self.metadata, submission, self.opts.table_names)

        if format_document:
            self.opts.target = utility.tidy_xml(self.opts.target, remove_source=True)

        logger.debug(f" ---> target file created: {self.opts.target}")

        return self.opts.target

    @utility.log_decorator(enter_message="Processing started...", exit_message="Processing done", level="DEBUG")
    def process(self, submission: int | str | Submission) -> None:
        """Process a submission. The submission can be either
        - an Excel file,
        - an XML file (generated by a previously processed Excel file)
        - a SubmissionData object (parsed Excel file, see importer/submission.py)
        - a submission id (int) already stored in the database
        """
        try:
            opts: Options = self.opts
            if opts.skip is True:
                logger.debug("Skipping: %s", opts.basename)
                return

            if isinstance(submission, Submission):

                if not self.specification.is_satisfied_by(submission):
                    logger.error(f" ---> {opts.basename} does not satisfy the specification")
                    raise SpecificationError(str(self.specification.messages))

                if self.opts.check_only:
                    logger.debug(f" ---> {opts.basename} satisfies the specification")
                    return

                if opts.dump_to_csv:
                    submission.to_csv(self.opts.output_folder)

            if opts.use_existing_submission:

                self.repository.remove(opts.submission_id, clear_header=False, clear_exploded=False)

            if not opts.use_existing_submission:

                opts.xml_filename = (
                    submission
                    if isinstance(submission, str)
                    else self.dispatch(submission, format_document=opts.tidy_xml)
                )

                if opts.register:
                    opts.submission_id = self.repository.register(
                        name=opts.submission_name,
                        source_name=opts.source_name,
                        data_types=opts.data_types,
                    )

                    self.repository.upload_xml(opts.xml_filename, opts.submission_id)
                    self.repository.extract_to_staging_tables(opts.submission_id)

            if opts.explode:

                self.repository.explode_to_public_tables(
                    opts.submission_id, p_dry_run=False, p_add_missing_columns=False
                )
                self.repository.set_pending(opts.submission_id)

        except SpecificationError:
            logger.error(f"Specification(s) not satisfied {opts.basename}")

        except Exception:
            logger.exception(f"aborted critical error {opts.basename}")
