import os
import xml.etree.ElementTree as ET
from typing import Iterator

import pandas as pd
import pytest

from importer.configuration.config import Config
from importer.metadata import Metadata
from importer.process import ImportService, Options
from importer.specification import SubmissionSpecification
from importer.submission import Submission
from importer.utility import create_db_uri


@pytest.fixture(scope="module")
def adna(cfg: Config) -> Iterator[Submission]:
    uri: str = create_db_uri(**cfg.get("options:database"))
    source: str = cfg.get("test:adna:source:filename")
    metadata: Metadata = Metadata(uri)
    submission: Submission = Submission.load(metadata=metadata, source=source)
    return submission


def test_to_lookups_sql(adna: Submission):

    filename: str = 'tests/output/lookups.sql'

    os.makedirs(os.path.dirname(filename), exist_ok=True)
    if os.path.isfile(filename):
        os.unlink(filename)

    adna.to_lookups_sql(filename)

    assert os.path.isfile(filename)


def test_loaded_adna_source(adna: Submission, cfg: Config):
    source: str = cfg.get("test:adna:source:filename")

    assert adna.data_tables is not None

    assert all(len(df) > 0 for df in adna.data_tables.values())

    # Verify that no table in the submission is keyed by excel sheet name for aliased tables
    assert all(n.excel_sheet not in adna.data_tables for n in adna.metadata.sead_schema.aliased_tables)

    with pd.ExcelFile(source) as reader:
        # Verify that all excel sheet names are in the submission data tables
        excel_sheet_names: set[str] = set(reader.sheet_names)
        excel_table_names: set[str] = {
            n for n, t in adna.metadata.sead_schema.items() if t.excel_sheet in excel_sheet_names
        }

        assert all(table_name in adna.data_tables for table_name in excel_table_names)


def test_adna_tables_specifications(adna: Submission, cfg: Config):
    specification: SubmissionSpecification = SubmissionSpecification(
        metadata=adna.metadata, ignore_columns=cfg.get("options:ignore_columns"), raise_errors=False
    )
    specification.is_satisfied_by(adna)
    assert specification.messages.errors == []


def test_dispatch_a_dna_submission_to_xml_file(adna: Submission, cfg: Config):

    opts: Options = Options(
        skip=False,
        filename=cfg.get("test:adna:source:filename"),
        data_types='adna',
        submission_id=None,
        database=cfg.get("options:database"),
        output_folder='tests/output',
    )

    if os.path.isfile(opts.target):
        os.remove(opts.target)

    service: ImportService = ImportService(metadata=adna.metadata, opts=opts)

    if os.path.isfile(opts.target):
        os.remove(opts.target)

    service.dispatch(adna, format_document=False)

    assert os.path.isfile(opts.target)

    with open(opts.target, "r", encoding="utf-8") as f:
        data: dict = xmltodict.parse(f.read())

    assert data is not None

    assert len(data['sead-data-upload'].keys()) == len(adna.data_tables)


def test_import_a_dna_submission(adna: Submission, cfg: Config):

    opts: Options = Options(
        **{
            'filename': cfg.get("test:adna:source:filename"),
            'data_types': 'adna',
            'database': cfg.get("options:database"),
            'output_folder': 'tests/output',
            'skip': False,
            'submission_id': None,
            'table_names': None,
            'xml_filename': None,
            'check_only': False,
            'register': True,
            'transfer_format': 'csv',
        }
    )

    if os.path.isfile(opts.target):
        os.remove(opts.target)

    service: ImportService = ImportService(metadata=adna.metadata, opts=opts)

    service.process(submission=adna)

    assert not service.specification.messages.errors

    assert os.path.isfile(opts.target)

    with open(opts.target, "r") as f:
        root: ET.Element = ET.fromstring(f.read())

    exported_java_classes: set[str] = {child.tag for child in root}

    assert 'TblContacts' in exported_java_classes

    expected_java_classes: set[str] = {adna.metadata[t].java_class for t in adna.data_table_names}

    assert all(t in exported_java_classes for t in expected_java_classes)
