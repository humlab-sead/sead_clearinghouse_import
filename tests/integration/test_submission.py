from os.path import isfile

import pytest

from importer.configuration import Config
from importer.metadata import SchemaService, SeadSchema
from importer.specification import SubmissionSpecification
from importer.submission import Submission
from importer.utility import create_db_uri
from tests.utility import generate_test_excel

# pylint: disable=too-many-statements,unused-argument,redefined-outer-name


# @pytest.mark.skipif(isfile(ConfigValue("test:reduced_excel_filename").resolve()), reason="Test file already exists")
def test_generate_test_excel(cfg: Config):

    if isfile(cfg.get("test:reduced_excel_filename")):
        return

    generate_test_excel(
        excel_filename=cfg.get("test:source_excel_filename"),
        test_sites=cfg.get("test:sites"),
        filename=cfg.get("test:reduced_excel_filename"),
    )


@pytest.mark.integration
@pytest.mark.skip(reason="Requires live database connection")
class TestSubmission:


    @pytest.fixture
    def service(self, cfg: Config) -> SchemaService:
        service: SchemaService = SchemaService(create_db_uri(**cfg.get("options:database")))
        return service

    @pytest.fixture
    def schema(self, service: SchemaService) -> SeadSchema:
        schema: SeadSchema = service.load()
        return schema

    def test_excel_is_loaded_correctly(self, submission: Submission):
        assert submission is not None
        assert submission.data_tables is not None
        assert len(submission.data_tables) > 0
        assert isinstance(submission.data_tables, dict)

    def test_contains(self, submission: Submission):
        assert "tbl_sites" in submission
        assert "tbl_dummy" not in submission

    def test_exists(self, submission: Submission):
        assert "tbl_sites" in submission
        assert "tbl_dummy" not in submission

    def test_data_tablenames(self, submission: Submission):
        assert "tbl_analysis_entities" in submission.data_table_names

    def test_has_system_id(self, submission: Submission):
        assert submission.has_system_id("tbl_sites")
        assert not submission.has_system_id("tbl_dummy")

    def test_referenced_keyset(self, submission: Submission, service: SchemaService, schema: SeadSchema):
        """Note that submission is areduced versions of the real things, so all references do not exists."""

        key_set: set[int] = submission.get_referenced_keyset(schema, "tbl_sites")
        assert key_set is not None
        # FIXME: Add real assertions here
        # assert {1, 2, 3, 4, 5} == submission.get_referenced_keyset(schema, "tbl_sites")
        # assert {10} == submission.get_referenced_keyset(schema, "tbl_methods")

    def test_tables_specifications(self, cfg: Config, submission: Submission, schema: SeadSchema):
        ignore_columns: list[str] = cfg.get("options:ignore_columns")
        specification: SubmissionSpecification = SubmissionSpecification(
            schema=schema, ignore_columns=ignore_columns
        )
        specification.is_satisfied_by(submission)
        assert specification.messages.errors == []
