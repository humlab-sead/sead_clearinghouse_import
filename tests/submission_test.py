from os.path import isfile

import pandas as pd
import pytest

from importer.configuration import ConfigValue
from importer.metadata import Metadata
from importer.specification import SubmissionSpecification
from importer.submission import SubmissionData
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


def test_excel_is_loaded_correctly(submission: SubmissionData):
    assert submission is not None
    assert submission.data_tables is not None
    assert len(submission.data_tables) > 0
    assert isinstance(submission.data_tables, dict)

    assert submission.data_table_index is not None
    assert isinstance(submission.data_table_index, pd.DataFrame)
    assert len(submission.data_table_index) > 0
    assert any(submission.data_table_index.only_new_data)
    assert any(submission.data_table_index.new_data)


def test_contains(submission: SubmissionData):
    assert "tbl_sites" in submission
    assert "tbl_dummy" not in submission


def test_exists(submission: SubmissionData):
    assert submission.exists("tbl_sites")
    assert not submission.exists("tbl_dummy")


def test_data_tablenames(submission: SubmissionData):
    assert "tbl_analysis_entities" in submission.data_table_names


def test_index_table_names(submission: SubmissionData):
    assert set(submission.index_table_names) == set(submission.data_table_names)


def test_has_system_id(submission: SubmissionData):
    assert submission.has_system_id("tbl_sites")
    assert not submission.has_system_id("tbl_dummy")


def test_referenced_keyset(submission: SubmissionData, metadata: Metadata):
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


def test_tables_specifications(submission: SubmissionData):
    metadata: Metadata = Metadata(get_db_uri())
    specifixation: SubmissionSpecification = SubmissionSpecification(metadata=metadata, ignore_columns=['date_updated'])
    specifixation.is_satisfied_by(submission)
    assert specifixation.messages.errors == []
