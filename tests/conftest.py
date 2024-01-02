import pandas as pd
import pytest

from importer.model import Metadata
from importer.utility import load_sead_data


@pytest.fixture
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
