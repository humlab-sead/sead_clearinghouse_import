import pandas as pd
import pytest

from importer.metadata import Metadata
from tests.utility import get_db_uri

# pylint: disable=redefined-outer-name,no-member


# def test_load_sql_from_file():
#     sql: str = load_sql_from_file("sead_tables")
#     assert "select" in sql.lower()
#     sql: str = load_sql_from_file("sead_columns")
#     assert "select" in sql.lower()


def test_metadata_load_metadata():
    metadata: Metadata = Metadata(get_db_uri())
    assert isinstance(metadata.sead_tables, pd.DataFrame)
    assert isinstance(metadata.sead_columns, pd.DataFrame)
    assert isinstance(metadata.sead_schema, dict)
    assert isinstance(metadata.foreign_keys, pd.DataFrame)


def test_tables_specifications(metadata: Metadata):
    assert isinstance(metadata.sead_schema, dict)

    assert len(metadata.sead_tables) == len(metadata.sead_schema)
    assert set(metadata.sead_tables.columns)  - {'columns', 'is_new'} == set(metadata.sead_schema['tbl_sites'].keys()) - {'columns', 'is_new'}

    assert 'columns' in metadata.sead_schema['tbl_sites'].keys()
    assert 'site_id' in metadata.sead_schema['tbl_sites'].columns.keys()
    assert len(metadata.sead_schema['tbl_sites'].columns) == len(
        metadata.sead_columns[metadata.sead_columns.table_name == 'tbl_sites']
    )
    assert metadata.sead_schema['tbl_sites'].columns['site_id'].is_pk is True
    assert metadata.sead_schema['tbl_sites'].columns['site_name'].is_pk is False
    assert metadata.sead_schema['tbl_locations'].columns['location_type_id'].is_fk is True
    assert metadata.sead_schema['tbl_locations'].columns['location_id'].is_fk is False
    assert metadata.sead_schema['tbl_locations'].columns['location_id'].is_pk is True


def test_is_pk(metadata: Metadata):
    assert metadata.is_pk('tbl_sites', 'site_id') is True
    assert metadata.is_pk('tbl_sites', 'site_name') is False
    assert metadata.is_pk('tbl_locations', 'location_type_id') is False
    assert metadata.is_pk('tbl_locations', 'location_id') is True


def test_is_fk(metadata: Metadata):
    assert metadata.is_fk('tbl_sites', 'site_id') is False
    assert metadata.is_fk('tbl_sites', 'site_name') is False
    assert metadata.is_fk('tbl_locations', 'location_type_id') is True
    assert metadata.is_fk('tbl_locations', 'location_id') is False


def test_get_tablenames_referencing():
    metadata: Metadata = Metadata(get_db_uri())

    assert set(metadata.get_tablenames_referencing('tbl_sites')) == {
        'tbl_sample_groups',
        'tbl_site_images',
        'tbl_site_locations',
        'tbl_site_natgridrefs',
        'tbl_site_other_records',
        'tbl_site_preservation_status',
        'tbl_site_references',
    }


@pytest.mark.parametrize(
    'values',
    [
        ('tbl_abundances', 'abundance_element_id', 'tbl_abundance_elements', 'TblAbundanceElements'),
        ('tbl_analysis_entities', 'physical_sample_id', 'tbl_physical_samples', 'TblPhysicalSamples'),
        ('tbl_datasets', 'master_set_id', 'tbl_dataset_masters', 'TblDatasetMasters'),
        ('tbl_datasets', 'project_id', 'tbl_projects', 'TblProjects'),
    ],
)
def test_foreign_keys(metadata: Metadata, values: list[str]):
    assert isinstance(metadata.foreign_keys, pd.DataFrame)
    assert len(metadata.foreign_keys) > 0
    assert (metadata.foreign_keys == values).all(axis=1).any()
