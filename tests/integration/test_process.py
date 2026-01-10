import filecmp
import os
import pickle

import pandas as pd
import pytest

from importer.configuration.config import Config
from importer.metadata import SchemaService, SeadSchema
from importer.process import ImportService, Options
from importer.submission import Submission


def test_create_options(cfg: Config):
    opts: Options = Options(
        **{
            "filename": "data/input/dummy.xlsx",
            "data_types": "dendrochronology",
            "database": cfg.get("options:database"),
            "output_folder": "data/output",
            "skip": False,
            "submission_id": None,
            "table_names": None,
            "xml_filename": None,
            "check_only": True,
            "timestamp": True,
        }
    )
    assert opts.basename == "dummy"
    assert opts.timestamp
    assert opts.target is not None
    assert opts.ignore_columns is not None
    assert opts.db_uri().startswith("postgresql+psycopg://")


@pytest.mark.integration
@pytest.mark.skip(reason="Requires live database connection")
class TestImportService:
    def test_import_reduced_submission(self, cfg: Config):
        target_filename: str = "data/output/building_dendro_reduced.xml"
        expected_filename: str = "tests/test_data/building_dendro_reduced.xml"

        opts: Options = Options(
            **{
                "filename": "tests/test_data/building_dendro_reduced.xlsx",
                "data_types": "dendrochronology",
                "database": cfg.get("options:database"),
                "output_folder": "data/output",
                "skip": False,
                "submission_id": None,
                "table_names": None,
                "xml_filename": None,
                "check_only": False,
                "register": False,
                "explode": False,
                "timestamp": False,
                "tidy_xml": False,
            }
        )
        assert opts.filename is not None

        schema_service: SchemaService = SchemaService(opts.db_uri())
        schema: SeadSchema = schema_service.load()
        submission: Submission = Submission.load(schema=schema, source=opts.filename, service=schema_service)

        service: ImportService = ImportService(schema=schema, opts=opts)
        service.process(submission=submission)
        assert len(service.specification.errors) == 0
        assert filecmp.cmp(target_filename, expected_filename, shallow=False)

