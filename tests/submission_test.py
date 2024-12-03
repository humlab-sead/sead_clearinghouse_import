from os.path import isfile

import pytest

from importer.configuration import ConfigValue
from importer.metadata import Metadata
from importer.specification import SubmissionSpecification
from importer.submission import Submission
from tests.utility import generate_test_excel, get_db_uri

# pylint: disable=too-many-statements,unused-argument,redefined-outer-name


@pytest.mark.skipif(isfile(ConfigValue("test:reduced_excel_filename").resolve()))
def test_generate_test_excel():
    submission_filename: str = ConfigValue("test:source_excel_filename").resolve()
    generate_test_excel(
        excel_filename=submission_filename,
        test_sites=ConfigValue("test:sites").resolve(),
        filename=ConfigValue("test:reduced_excel_filename").resolve(),
    )


def test_excel_is_loaded_correctly(submission: Submission):
    assert submission is not None
    assert submission.data_tables is not None
    assert len(submission.data_tables) > 0
    assert isinstance(submission.data_tables, dict)


def test_contains(submission: Submission):
    assert "tbl_sites" in submission
    assert "tbl_dummy" not in submission


def test_exists(submission: Submission):
    assert 'tbl_sites' in submission
    assert not "tbl_dummy" in submission


def test_data_tablenames(submission: Submission):
    assert "tbl_analysis_entities" in submission.data_table_names


def test_has_system_id(submission: Submission):
    assert submission.has_system_id("tbl_sites")
    assert not submission.has_system_id("tbl_dummy")


def test_referenced_keyset(submission: Submission, metadata: Metadata):
    """Note that submission is areduced versions of the real things, so all references do not exists."""

    def compute_unique_system_ids_referenced_by_fk(table_name: str, pk_name: str) -> int:
        fk_tables: list[str] = metadata.get_tablenames_referencing(table_name)
        unique_ids: set[str] = set()
        for fk_table_name in fk_tables:
            if fk_table_name in submission:
                unique_ids.update(set(submission[fk_table_name][pk_name].unique()))
        return unique_ids

    unique_site_ids: set[str] = compute_unique_system_ids_referenced_by_fk('tbl_sites', 'site_id')

    assert {1635} == unique_site_ids == submission.get_referenced_keyset(metadata, 'tbl_sites')

    assert {10} == submission.get_referenced_keyset(metadata, 'tbl_methods')


def test_tables_specifications(submission: Submission):
    metadata: Metadata = Metadata(get_db_uri())
    ignore_columns: list[str] = ConfigValue("options:ignore_columns").resolve()
    specifixation: SubmissionSpecification = SubmissionSpecification(metadata=metadata, ignore_columns=ignore_columns)
    specifixation.is_satisfied_by(submission)
    assert specifixation.messages.errors == []
