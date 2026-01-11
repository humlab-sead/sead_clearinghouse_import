from typing import Iterator

import pandas as pd
import pytest
from dotenv import load_dotenv

from importer.configuration import ConfigStore
from importer.configuration.interface import ConfigLike
from importer.metadata import MockSchemaService, SchemaService, SeadSchema
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
def cfg() -> ConfigLike:
    ConfigStore.get_instance().configure_context(
        source=CONFIG_FILENAME, env_filename=DOTENV_FILENAME, env_prefix=ENV_PREFIX
    )
    return ConfigStore.get_instance().config()  # type: ignore[return-value]


@pytest.fixture(scope="session")
def schema(cfg: ConfigLike) -> Iterator[SeadSchema]:
    # FIXME: We need to mock this! Loading live metadata makes tests fragile.
    # The SchemaService can be mocked using CSV files in test_data.
    service: SchemaService = SchemaService(create_db_uri(**cfg.get("options:database")))
    schema: SeadSchema = service.load()
    yield schema

@pytest.fixture(scope="session")
def service(cfg: ConfigLike) -> Iterator[SchemaService]:
    sead_tables: pd.DataFrame = pd.read_csv("tests/test_data/source_tables.csv")
    sead_columns: pd.DataFrame = pd.read_csv("tests/test_data/source_columns.csv")

    service: SchemaService = MockSchemaService(sead_tables=sead_tables, sead_columns=sead_columns)
    yield service

@pytest.fixture(scope="session")
def submission(schema: SeadSchema, cfg: ConfigLike, service: SchemaService) -> Iterator[Submission]:
    yield Submission.load(schema=schema, source=cfg.get("test:reduced_excel_filename"), service=service)
