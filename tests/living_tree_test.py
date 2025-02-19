import os
import xml.etree.ElementTree as ET
from typing import Iterator

import pandas as pd
import pytest

from importer.configuration.config import Config
from importer.metadata import Metadata
from importer.process import ImportService, Options
from importer.specification import ForeignKeyColumnsHasValuesSpecification, SpecificationMessages, SubmissionSpecification
from importer.submission import Submission
from importer.utility import create_db_uri


@pytest.fixture(scope="module")
def living_tree(cfg: Config) -> Iterator[Submission]:
    db_opts = cfg.get("options:database") | cfg.get("test:dendrochronology:database")
    uri: str = create_db_uri(**db_opts)
    source: str = cfg.get("test:dendrochronology:living_tree:source:filename")
    metadata: Metadata = Metadata(uri)
    submission: Submission = Submission.load(metadata=metadata, source=source)
    return submission


def test_load_living_tree(living_tree: Submission):
    assert living_tree is not None
    assert living_tree.metadata is not None
    assert living_tree.data_tables is not None
    assert len(living_tree.data_tables) > 0


def test_living_tree_tables_specifications(living_tree: Submission, cfg: Config):
    specification: SubmissionSpecification = SubmissionSpecification(
        metadata=living_tree.metadata, ignore_columns=cfg.get("options:ignore_columns"), raise_errors=False
    )
    specification.is_satisfied_by(living_tree)
    assert specification.messages.errors == []

def test_living_tree_tables_specifications_bugg(living_tree: Submission, cfg: Config):
    specification: ForeignKeyColumnsHasValuesSpecification = ForeignKeyColumnsHasValuesSpecification(
        metadata=living_tree.metadata, 
        messages= SpecificationMessages(),
        ignore_columns=cfg.get("options:ignore_columns")
    )
    specification.is_satisfied_by(living_tree, 'tbl_dataset_submissions')
    assert specification.messages.errors == []
    assert specification.messages.warnings == []


def test_to_lookups_sql(living_tree: Submission):

    filename: str = 'tests/output/lookups.sql'

    os.makedirs(os.path.dirname(filename), exist_ok=True)
    if os.path.isfile(filename):
        os.unlink(filename)

    living_tree.to_lookups_sql(filename)

    assert os.path.isfile(filename)


def test_loaded_living_tree_source(living_tree: Submission, cfg: Config):
    source: str = cfg.get("test:dendrochronology:living_tree:source:filename")

    assert living_tree.data_tables is not None

    empty_tables: list[str] = [n for n, df in living_tree.data_tables.items() if len(df) == 0]

    assert len(empty_tables) == 0, f"Empty tables found: {empty_tables}"

    # Verify that no table in the submission is keyed by excel sheet name for aliased tables
    assert all(n.excel_sheet not in living_tree.data_tables for n in living_tree.metadata.sead_schema.aliased_tables)

    with pd.ExcelFile(source) as reader:
        # Verify that all excel sheet names are in the submission data tables
        excel_sheet_names: set[str] = set(reader.sheet_names)
        excel_table_names: set[str] = {
            n for n, t in living_tree.metadata.sead_schema.items() if t.excel_sheet in excel_sheet_names
        }

        assert all(table_name in living_tree.data_tables for table_name in excel_table_names)



# def test_dispatch_a_dna_submission_to_xml_file(living_tree: Submission, cfg: Config):

#     opts: Options = Options(
#         skip=False,
#         filename=cfg.get("test:dendrochronology:living_tree:source:filename"),
#         data_types='living_tree',
#         submission_id=None,
#         database=cfg.get("options:database"),
#         output_folder='tests/output',
#     )

#     if os.path.isfile(opts.target):
#         os.remove(opts.target)

#     service: ImportService = ImportService(metadata=living_tree.metadata, opts=opts)

#     if os.path.isfile(opts.target):
#         os.remove(opts.target)

#     service.dispatch(living_tree, format_document=False)

#     assert os.path.isfile(opts.target)

#     with open(opts.target, "r", encoding="utf-8") as f:
#         data: dict = xmltodict.parse(f.read())

#     assert data is not None

#     assert len(data['sead-data-upload'].keys()) == len(living_tree.data_tables)


def test_import_a_dna_submission(living_tree: Submission, cfg: Config):

    opts: Options = Options(
        **{
            'filename': cfg.get("test:dendrochronology:living_tree:source:filename"),
            'data_types': 'living_tree',
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

    service: ImportService = ImportService(metadata=living_tree.metadata, opts=opts)

    service.process(submission=living_tree)

    assert not service.specification.messages.errors

    assert os.path.isfile(opts.target)

    with open(opts.target, "r") as f:
        root: ET.Element = ET.fromstring(f.read())

    exported_java_classes: set[str] = {child.tag for child in root}

    assert 'TblContacts' in exported_java_classes

    expected_java_classes: set[str] = {living_tree.metadata[t].java_class for t in living_tree.data_table_names}

    assert all(t in exported_java_classes for t in expected_java_classes)
