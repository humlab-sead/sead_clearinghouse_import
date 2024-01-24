import pandas as pd
import pytest

from importer.metadata import Metadata
from importer.submission import SubmissionData, load_excel
from importer.utility import load_sead_data

from . import REDUCED_EXCEL_FILENAME

# pylint: disable=redefined-outer-name


@pytest.fixture(scope="session")
def metadata() -> Metadata:
    instance = Metadata("a-dummy-db-uri")
    instance.__dict__['sead_tables'] = load_sead_data(
        "",
        pd.read_json('tests/test_data/sead_tables.json'),
        ["table_name"],
    )
    instance.__dict__['sead_columns'] = load_sead_data(
        "",
        pd.read_json('tests/test_data/sead_columns.json'),
        ["table_name", "column_name"],
        ["table_name", "position"],
    )

    return instance


@pytest.fixture(scope="session")
def submission(metadata: Metadata) -> SubmissionData:
    return load_excel(metadata=metadata, source=REDUCED_EXCEL_FILENAME)
