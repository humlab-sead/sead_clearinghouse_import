from os.path import isfile
import pandas as pd

import pytest

from importer.model import Metadata
from importer.model.specification import SubmissionSpecification
from importer.model.submission import SubmissionData, load_excel
from tests.utility import dburi_from_env, generate_test_excel

TEST_SITES: list[int] = [1635]
SOURCE_EXCEL_FILENAME: str = "data/input/building_dendro_2023-12_import.xlsx"
REDUCED_EXCEL_FILENAME: str = "tests/test_data/building_dendro_reduced.xlsx"

# pylint: disable=too-many-statements,unused-argument,redefined-outer-name


@pytest.fixture(scope="module")
def metadata() -> Metadata:
    # FIXME: Create a test fixture for Metadata instead of using the real thing
    return Metadata(dburi_from_env())


@pytest.fixture(scope="module")
def submission(metadata: Metadata) -> SubmissionData:
    return load_excel(metadata=metadata, source=REDUCED_EXCEL_FILENAME)


@pytest.mark.skipif(isfile(REDUCED_EXCEL_FILENAME), reason="Test file already exists")
def test_generate_test_excel():
    submission_filename: str = SOURCE_EXCEL_FILENAME
    generate_test_excel(
        excel_filename=submission_filename,
        test_sites=TEST_SITES,
        filename=REDUCED_EXCEL_FILENAME,
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
    metadata: Metadata = Metadata(dburi_from_env())
    specifixation: SubmissionSpecification = SubmissionSpecification(metadata=metadata, ignore_columns=['date_updated'])
    specifixation.is_satisfied_by(submission)
    assert specifixation.messages.errors == []
