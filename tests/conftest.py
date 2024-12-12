import pytest

from importer.configuration import ConfigStore
from importer.configuration.inject import ConfigValue
from importer.metadata import Metadata
from importer.submission import Submission
from tests.utility import get_db_uri

# pylint: disable=redefined-outer-name

ConfigStore.configure_context(
    source="tests/test_data/config.yml", env_filename="tests/test_data/.env", env_prefix="SEAD_IMPORT"
)


@pytest.fixture(scope="session")
def metadata() -> Metadata:
    metadata: Metadata = Metadata(get_db_uri())
    return metadata

    # instance = Metadata("a-dummy-db-uri")
    # instance.__dict__['sead_tables'] = load_sead_data(
    #     "",
    #     pd.read_json('tests/test_data/sead_tables.json'),
    #     ["table_name"],
    # )
    # instance.__dict__['sead_columns'] = load_sead_data(
    #     "",
    #     pd.read_json('tests/test_data/sead_columns.json'),
    #     ["table_name", "column_name"],
    #     ["table_name", "position"],
    # )

    # return instance


@pytest.fixture(scope="session")
def submission(metadata: Metadata) -> Submission:
    return Submission.load(metadata=metadata, source=ConfigValue("test:reduced_excel_filename").resolve())
