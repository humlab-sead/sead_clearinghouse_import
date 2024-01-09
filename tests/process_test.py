import filecmp
import os
import pickle

import pandas as pd

from importer.model.metadata import Metadata
from importer.model.submission import SubmissionData, load_excel
from importer.process import ImportService, Options
from importer.scripts.import_excel import workflow


def test_create_options():
    opts: Options = Options(
        **{
            'filename': 'data/input/building_dendro_2023-12_import.xlsx',
            'data_types': 'dendrochronology',
            'dbhost': 'host',
            'dbname': 'database_name',
            'dbuser': 'username',
            'output_folder': 'data/output',
            'port': 5432,
            'skip': False,
            'submission_id': None,
            'table_names': None,
            'xml_filename': None,
            'check_only': True,
            'log_folder': './logs',
            'timestamp': True,
        }
    )
    assert opts.basename == 'building_dendro_2023-12_import'
    assert opts.timestamp
    assert opts.target is not None
    assert opts.ignore_columns is not None
    assert opts.db_uri().startswith('postgresql://')


def test_import_submission():
    opts: Options = Options(
        **{
            'filename': 'data/input/building_dendro_2023-12_import.xlsx',
            'data_types': 'dendrochronology',
            'dbhost': 'humlabseadserv.srv.its.umu.se',
            'dbname': 'sead_staging_202212',
            'dbuser': 'humlab_admin',
            'output_folder': 'data/output',
            'port': 5432,
            'skip': False,
            'submission_id': None,
            'table_names': None,
            'xml_filename': None,
            'check_only': False,
            'log_folder': './logs',
        }
    )
    metadata: Metadata = Metadata(opts.db_uri())

    submission: SubmissionData = load_or_cache_submission(opts, metadata)

    ImportService(metadata=metadata, opts=opts).process(submission=submission)


def test_import_reduced_submission():
    target_filename: str = 'data/output/building_dendro_reduced.xml'
    expected_filename: str = 'tests/test_data/building_dendro_reduced.xml'

    opts: Options = Options(
        **{
            'filename': 'tests/test_data/building_dendro_reduced.xlsx',
            'data_types': 'dendrochronology',
            'dbhost': 'humlabseadserv.srv.its.umu.se',
            'dbname': 'sead_staging_202212',
            'dbuser': 'humlab_admin',
            'output_folder': 'data/output',
            'port': 5432,
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

    ImportService(metadata=metadata, opts=opts).process(submission=submission)

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
