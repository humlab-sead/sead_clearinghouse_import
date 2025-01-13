from importer.configuration.config import Config
from importer.metadata import Metadata
from importer.process import ImportService, Options
from importer.specification import SubmissionSpecification
from importer.submission import Submission
from importer.utility import create_db_uri


def test_load_adna_source(cfg: Config):
    uri: str = create_db_uri(**cfg.get("options:database"))
    source: str = cfg.get("test:adna:source:filename")
    metadata: Metadata = Metadata(uri)
    submission: Submission = Submission.load(metadata=metadata, source=source)

    assert submission is not None
    assert submission.data_tables is not None

    assert all(len(df) > 0 for df in submission.data_tables.values())


def test_adna_tables_specifications(cfg: Config):
    uri: str = create_db_uri(**cfg.get("options:database"))
    source: str = cfg.get("test:adna:source:filename")
    ignore_columns: list[str] = cfg.get("options:ignore_columns")
    metadata: Metadata = Metadata(uri)
    submission: Submission = Submission.load(metadata=metadata, source=source)
    specification: SubmissionSpecification = SubmissionSpecification(
        metadata=metadata, ignore_columns=ignore_columns, raise_errors=False
    )
    specification.is_satisfied_by(submission)
    assert specification.messages.errors == []


def test_import_a_dna_submission(cfg: Config):
    opts: Options = Options(
        **{
            'filename': cfg.get("test:adna:source:filename"),
            'data_types': 'dendrochronology',
            'database': cfg.get("options:database"),
            'output_folder': 'data/output',
            'skip': False,
            'submission_id': None,
            'table_names': None,
            'xml_filename': None,
            'check_only': False,
        }
    )
    metadata: Metadata = Metadata(opts.db_uri())

    assert metadata is not None

    assert 'tbl_analysis_values' in metadata.sead_tables.table_name.values

    assert metadata.sead_schema.lookup_tables is not None

    submission: Submission = Submission.load(
        metadata=metadata, source=opts.filename
    )  # load_or_cache_submission(opts, metadata)

    ImportService(metadata=metadata, opts=opts).process(submission=submission)
