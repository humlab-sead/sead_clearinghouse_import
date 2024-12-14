from typing import Any, Iterator

import pytest
from dotenv import load_dotenv

from importer.configuration import ConfigStore
from importer.configuration.config import Config
from importer.metadata import Metadata
from importer.submission import Submission
from importer.utility import create_db_uri

# pylint: disable=redefined-outer-name

# @pytest.fixture(scope="session")
# def cfg() -> Iterator[Config]:
#     ConfigStore.configure_context(
#         source="tests/test_data/config.yml", env_filename="tests/test_data/.env", env_prefix="SEAD_IMPORT"
#     )
#     yield ConfigStore.config()

DOTENV_FILENAME = "tests/test_data/.env"
CONFIG_FILENAME = "tests/test_data/config.yml"
ENV_PREFIX = "SEAD_IMPORT"

load_dotenv(DOTENV_FILENAME)


@pytest.fixture(scope="session")
def cfg() -> Config:
    ConfigStore.configure_context(source=CONFIG_FILENAME, env_filename=DOTENV_FILENAME, env_prefix=ENV_PREFIX)
    return ConfigStore.config()


@pytest.fixture(scope="session")
def metadata(cfg: Config) -> Iterator[Metadata]:
    metadata: Metadata = Metadata(create_db_uri(**cfg.get("options:database")))
    yield metadata

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
def submission(metadata: Metadata, cfg: Config) -> Iterator[Submission]:
    yield Submission.load(metadata=metadata, source=cfg.get("test:reduced_excel_filename"))
