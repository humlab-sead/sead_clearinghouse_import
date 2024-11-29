from importer.configuration import ConfigValue
from importer.metadata import Metadata
from importer.process import ImportService, Options
from importer.specification import SubmissionSpecification
from importer.submission import SubmissionData, load_excel
from importer.utility import create_db_uri
from tests.process_test import load_or_cache_submission

ADNA_TABLENAMES: set[str] = {
    'tbl_value_classes',
    'tbl_value_types',
    'tbl_value_type_items',
    'tbl_analysis_values',
    'tbl_sites',
    'tbl_dataset_contacts',
    'tbl_dataset_submissions',
    'tbl_site_locations',
    'tbl_site_references',
    'tbl_sample_groups',
    'tbl_physical_samples',
    'tbl_sample_alt_refs',
    'tbl_datasets',
    'tbl_analysis_entities',
    'tbl_relative_dates',
    'tbl_abundances',
    'tbl_taxa_tree_orders',
    'tbl_taxonomic_order_systems',
    'tbl_projects',
    'tbl_taxa_tree_families',
    'tbl_taxa_tree_genera',
    'tbl_taxa_tree_master',
    'tbl_taxonomic_order',
    'tbl_biblio',
    'tbl_record_types',
    'tbl_methods',
    'tbl_alt_ref_types',
    'tbl_contacts',
    'tbl_dataset_masters',
    'tbl_locations',
}


def test_load_adna_source():
    uri: str = create_db_uri(**ConfigValue("test:adna:database").resolve())
    source: str = ConfigValue("test:adna:source:filename").resolve()
    metadata: Metadata = Metadata(uri)
    submission: SubmissionData = load_excel(metadata=metadata, source=source)

    assert submission is not None
    assert submission.data_tables is not None
    assert set(submission.data_tables.keys()) == ADNA_TABLENAMES

    assert all(len(df) > 0 for df in submission.data_tables.values())

    assert submission.data_table_index is not None

    assert len(submission.data_tables) == len(submission.data_table_index)
    assert len(submission.data_tables) == len(submission.data_table_names)


def test_adna_tables_specifications():
    uri: str = create_db_uri(**ConfigValue("test:adna:database").resolve())
    source: str = ConfigValue("test:adna:source:filename").resolve()
    metadata: Metadata = Metadata(uri)
    submission: SubmissionData = load_excel(metadata=metadata, source=source)
    specification: SubmissionSpecification = SubmissionSpecification(metadata=metadata, ignore_columns=['date_updated'])
    specification.is_satisfied_by(submission)
    assert specification.messages.errors == []


def test_import_a_dna_submission():
    opts: Options = Options(
        **{
            'filename': ConfigValue("test:adna:source:filename").resolve(),
            'data_types': 'dendrochronology',
            'db_opts': ConfigValue("test:adna:database").resolve(),
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
