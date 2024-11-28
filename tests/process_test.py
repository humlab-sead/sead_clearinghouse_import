import filecmp
import os
import pickle

import pandas as pd

from importer.configuration.inject import ConfigValue
from importer.metadata import Metadata
from importer.process import ImportService, Options
from importer.submission import SubmissionData, load_excel


def test_create_options():
    opts: Options = Options(
        **{
            'filename': 'data/input/dummy.xlsx',
            'data_types': 'dendrochronology',
            'db_opts': ConfigValue("options:db_opts").resolve(),
            'output_folder': 'data/output',
            'skip': False,
            'submission_id': None,
            'table_names': None,
            'xml_filename': None,
            'check_only': True,
            'log_folder': './logs',
            'timestamp': True,
        }
    )
    assert opts.basename == 'dummy'
    assert opts.timestamp
    assert opts.target is not None
    assert opts.ignore_columns is not None
    assert opts.db_uri().startswith('postgresql://')


def test_import_a_dna_submission():
    opts: Options = Options(
        **{
            'filename': 'data/input/SEAD_aDNA_data_20241114_RM.xlsx',
            'data_types': 'dendrochronology',
            'db_opts': ConfigValue("options:db_opts").resolve(),
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

    submission: SubmissionData = load_or_cache_submission(opts, metadata)

    ImportService(metadata=metadata, opts=opts).process(submission=submission)


def test_import_reduced_submission():
    target_filename: str = 'data/output/building_dendro_reduced.xml'
    expected_filename: str = 'tests/test_data/building_dendro_reduced.xml'

    opts: Options = Options(
        **{
            'filename': 'tests/test_data/building_dendro_reduced.xlsx',
            'data_types': 'dendrochronology',
            'db_opts': ConfigValue("options:db_opts").resolve(),
            'output_folder': 'data/output',
            'skip': False,
            'submission_id': None,
            'table_names': None,
            'xml_filename': None,
            'check_only': False,
            'register': False,
            'explode': False,
            'log_folder': './logs',
            'timestamp': False,
            "tidy_xml": False,
        }
    )

    metadata: Metadata = Metadata(opts.db_uri())

    submission: SubmissionData = load_excel(metadata=metadata, source=opts.filename)

    service: ImportService = ImportService(metadata=metadata, opts=opts)
    service.process(submission=submission)
    assert len(service.specification.errors) == 0
    assert filecmp.cmp(target_filename, expected_filename, shallow=False)


def load_or_cache_submission(opts, metadata) -> SubmissionData:
    pickled_filename: str = f"{opts.basename}.pkl"
    if not os.path.isfile(pickled_filename):
        submission: SubmissionData = load_excel(metadata=metadata, source=opts.filename)
        with open(pickled_filename, "wb") as fp:
            pickle.dump(submission, fp)
    else:
        with open(pickled_filename, "rb") as fp:
            submission: dict[str, pd.DataFrame] = pickle.load(fp)
    return submission
