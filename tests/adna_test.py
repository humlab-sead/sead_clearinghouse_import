import os
import pandas as pd
from importer.configuration.config import Config
from importer.metadata import Metadata, SeadSchema
from importer.process import ImportService, Options
from importer.specification import SubmissionSpecification
from importer.submission import Submission
from importer.utility import create_db_uri
import xml.etree.ElementTree as ET


def test_load_adna_source(cfg: Config):
    uri: str = create_db_uri(**cfg.get("options:database"))
    source: str = cfg.get("test:adna:source:filename")
    metadata: Metadata = Metadata(uri)
    sead_schema: SeadSchema = metadata.sead_schema
    submission: Submission = Submission.load(metadata=metadata, source=source)

    assert submission is not None
    assert submission.data_tables is not None

    assert all(len(df) > 0 for df in submission.data_tables.values())

    # Verify that no table in the submission is keyed by excel sheet name for aliased tables
    assert all(n.excel_sheet not in submission.data_tables for n in sead_schema.aliased_tables)

    with pd.ExcelFile(source) as reader:
        # Verify that all excel sheet names are in the submission data tables
        excel_sheet_names: set[str] = set(reader.sheet_names)
        excel_table_names: set[str] = {n for n, t in sead_schema.items() if t.excel_sheet in excel_sheet_names}

        assert all(table_name in submission.data_tables for table_name in excel_table_names)


def test_adna_tables_specifications(cfg: Config):
    uri: str = create_db_uri(**cfg.get("options:database"))
    source: str = cfg.get("test:adna:source:filename")
    ignore_columns: list[str] = cfg.get("options:ignore_columns")
    metadata: Metadata = Metadata(uri)
    submission: Submission = Submission.load(metadata=metadata, source=source)
    specification: SubmissionSpecification = SubmissionSpecification(
        metadata=metadata, ignore_columns=ignore_columns, raise_errors=False
    )
    specification.is_satisfied_by(submission)
    assert specification.messages.errors == []


def test_import_a_dna_submission(cfg: Config):

    opts: Options = Options(
        **{
            'filename': cfg.get("test:adna:source:filename"),
            'data_types': 'dendrochronology',
            'database': cfg.get("options:database"),
            'output_folder': 'data/output',
            'skip': False,
            'submission_id': None,
            'table_names': None,
            'xml_filename': None,
            'check_only': False,
        }
    )
    metadata: Metadata = Metadata(opts.db_uri())

    assert metadata is not None

    assert 'tbl_analysis_values' in metadata.sead_tables.table_name.values

    assert metadata.sead_schema.lookup_tables is not None

    submission: Submission = Submission.load(
        metadata=metadata, source=opts.filename
    )  # load_or_cache_submission(opts, metadata)

    assert 'tbl_contacts' in submission.data_tables

    if os.path.isfile(opts.target):
        os.remove(opts.target)

    ImportService(metadata=metadata, opts=opts).process(submission=submission)

    assert os.path.isfile(opts.target)

    with open(opts.target, "r") as f:
        root: ET.Element = ET.fromstring(f.read())

    exported_java_classes: set[str] = {child.tag for child in root}

    assert 'TblContacts' in exported_java_classes

    expected_java_classes: set[str] = {metadata[t].java_class for t in submission.data_table_names}

    assert all(t in exported_java_classes for t in expected_java_classes)
