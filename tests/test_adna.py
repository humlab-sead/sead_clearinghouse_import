from importer.configuration import ConfigValue
from importer.metadata import Metadata
from importer.process import ImportService, Options
from importer.specification import SubmissionSpecification
from importer.submission import Submission
from importer.utility import create_db_uri
from tests.process_test import load_or_cache_submission


def test_load_adna_source():
    uri: str = create_db_uri(**ConfigValue("test:adna:database").resolve())
    source: str = ConfigValue("test:adna:source:filename").resolve()
    metadata: Metadata = Metadata(uri)
    submission: Submission = Submission.load(metadata=metadata, source=source)

    assert submission is not None
    assert submission.data_tables is not None

    assert all(len(df) > 0 for df in submission.data_tables.values())


def test_adna_tables_specifications():
    uri: str = create_db_uri(**ConfigValue("test:adna:database").resolve())
    source: str = ConfigValue("test:adna:source:filename").resolve()
    ignore_columns: list[str] = ConfigValue("options:ignore_columns").resolve()
    metadata: Metadata = Metadata(uri)
    submission: Submission = Submission.load(metadata=metadata, source=source)
    specification: SubmissionSpecification = SubmissionSpecification(
        metadata=metadata, ignore_columns=ignore_columns, raise_errors=False
    )
    specification.is_satisfied_by(submission)
    assert specification.messages.errors == []


def test_import_a_dna_submission():
    opts: Options = Options(
        **{
            'filename': ConfigValue("test:adna:source:filename").resolve(),
            'data_types': 'dendrochronology',
            'database': ConfigValue("test:adna:database").resolve(),
            'output_folder': 'data/output',
            'skip': False,
            'submission_id': None,
            'table_names': None,
            'xml_filename': None,
            'check_only': False,
            'log_folder': './logs',
        }
    )
    metadata: Metadata = Metadata(opts.db_uri())

    assert metadata is not None

    assert 'tbl_analysis_values' in metadata.sead_tables.table_name.values

    assert metadata.sead_schema.lookup_tables is not None

    submission: Submission = Submission.load(metadata=metadata, source=opts.filename) # load_or_cache_submission(opts, metadata)

    ImportService(metadata=metadata, opts=opts).process(submission=submission)
