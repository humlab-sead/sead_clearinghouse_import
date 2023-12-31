import json
from os.path import isfile
from unittest.mock import patch

import pandas as pd
import pytest

import importer
from importer.model.metadata import Metadata
from importer.utility import dburi_from_env, load_sead_data, load_sql_from_file  # Assuming Metadata is the class name

# pylint: disable=redefined-outer-name,no-member

TEST_TABLES: list[str] = [
    # 'tbl_sites',
    'tbl_locations',
    'tbl_sample_group_sampling_contexts',
    'tbl_record_types',
    'tbl_analysis_entities',
    'tbl_dataset_contacts',
    'tbl_dataset_submissions',
    'tbl_datasets',
    'tbl_dendro',
    'tbl_dendro_date_notes',
    'tbl_dendro_dates',
    'tbl_physical_samples',
    'tbl_projects',
    'tbl_sample_alt_refs',
    'tbl_sample_descriptions',
    'tbl_sample_group_coordinates',
    'tbl_sample_group_descriptions',
    'tbl_sample_group_notes',
    'tbl_sample_groups',
    'tbl_sample_locations',
    'tbl_sample_notes',
    'tbl_site_locations',
    'tbl_site_references',
    'tbl_sites',
    'tbl_abundances',
]


@pytest.fixture
def metadata() -> Metadata:
    instance = importer.model.metadata.Metadata("a-dummy-db-uri")
    instance.__dict__['sead_tables'] = load_sead_data(
        "",
        pd.read_json('tests/test_data/sead_tables.json'),
        ["table_name"],
    )
    instance.__dict__['sead_columns'] = load_sead_data(
        "",
        pd.read_json('tests/test_data/sead_columns.json'),
        ["table_name", "column_name"],
        ["table_name", "position"],
    )

    return instance


def test_load_metadata_from_postgres():
    metadata: Metadata = Metadata(dburi_from_env())

    with open('tests/test_data/sead_tables.json', 'w') as outfile:
        data: dict = metadata.sead_tables[metadata.sead_tables.table_name.isin(TEST_TABLES)].to_dict('records')
        json.dump(data, outfile, indent=4)

    with open('tests/test_data/sead_columns.json', 'w') as outfile:
        data: dict = metadata.sead_columns.fillna(0)[metadata.sead_columns.table_name.isin(TEST_TABLES)].to_dict(
            'records'
        )
        json.dump(data, outfile, indent=4)

    assert isinstance(metadata, Metadata)
    assert isinstance(metadata.sead_tables, pd.DataFrame)
    assert isinstance(metadata.sead_columns, pd.DataFrame)
    assert isinstance(metadata.sead_tables, pd.DataFrame)
    assert isinstance(metadata.sead_schema, dict)


def test_load_sql_from_file():
    sql: str = load_sql_from_file("sead_tables")
    assert "select" in sql.lower()
    sql: str = load_sql_from_file("sead_columns")
    assert "select" in sql.lower()


def test_tables_specifications(metadata: Metadata):
    assert isinstance(metadata.sead_schema, dict)

    assert len(metadata.sead_tables) == len(metadata.sead_schema)
    assert set(metadata.sead_tables.columns) | {'columns'} == set(metadata.sead_schema['tbl_sites'].keys())

    assert 'columns' in metadata.sead_schema['tbl_sites'].keys()
    assert 'site_id' in metadata.sead_schema['tbl_sites']['columns'].keys()
    assert len(metadata.sead_schema['tbl_sites']['columns']) == len(
        metadata.sead_columns[metadata.sead_columns.table_name == 'tbl_sites']
    )
    assert metadata.sead_schema['tbl_sites']['columns']['site_id']['is_pk'] is True
    assert metadata.sead_schema['tbl_sites']['columns']['site_name']['is_pk'] is False
    assert metadata.sead_schema['tbl_locations']['columns']['location_type_id']['is_fk'] is True
    assert metadata.sead_schema['tbl_locations']['columns']['location_id']['is_fk'] is False
    assert metadata.sead_schema['tbl_locations']['columns']['location_id']['is_pk'] is True


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


@pytest.mark.skipif(not isfile('data/metadata_20231223.xlsx'), reason='metadata_20231223.xlsx not found')
def test_regression_of_foreign_keys(metadata: Metadata):
    excel_columns: pd.DataFrame = regression_load('data/metadata_20231223.xlsx')
    expected_foreign_keys: pd.DataFrame = excel_columns['foreign_keys']
    expected_foreign_keys.columns = metadata.foreign_keys.columns

    """
    Merge the two dataframes using all columns (not index) and add a column to indicate
    if the row is in the left, right or both dataframes.
    """
    merged: pd.DataFrame = metadata.foreign_keys.merge(expected_foreign_keys, how='left', indicator=True)

    """We know that thes columns will differ, so we remove them from the comparison."""
    merged = merged[~merged.column_name.isin(['updated_dataset_id', 'error_uncertainty_id'])]

    assert (merged['_merge'] == 'both').all()

    # self.sead_columns[self.sead_columns.is_fk][['table_name', 'column_name', 'f_table_name', 'class_name']]


###### REMOVE CODE BELOW WHEN DONE
@pytest.mark.skipif(not isfile('data/metadata_20231223.xlsx'), reason='metadata_20231223.xlsx not found')
def test_load_by_sql_is_same_as_load_by_excel(metadata: Metadata):
    excel_columns: pd.DataFrame = regression_load('data/metadata_20231223.xlsx')['columns']
    excel_columns = excel_columns[excel_columns.table_name.isin(TEST_TABLES)]

    assert isinstance(metadata.sead_columns, pd.DataFrame)

    assert len(metadata.sead_columns.shape) == len(excel_columns.shape)

    for table_name in TEST_TABLES:
        # for table_name in ['tbl_dendro_dates']: #TEST_TABLES:

        if table_name == 'tbl_dendro_dates':
            continue

        expected_columns: pd.DataFrame = excel_columns[excel_columns.table_name == table_name]
        actual_columns: pd.DataFrame = metadata.sead_columns[metadata.sead_columns.table_name == table_name]

        assert len(expected_columns) == len(actual_columns), table_name

        assert actual_columns.table_name.tolist() == expected_columns.table_name.tolist(), table_name
        assert actual_columns.column_name.tolist() == expected_columns.column_name.tolist(), table_name
        assert actual_columns.position.tolist() == expected_columns.position.tolist(), table_name
        assert actual_columns.is_nullable.tolist() == [
            x == "YES" for x in expected_columns.nullable.tolist()
        ], table_name
        assert actual_columns.data_type.tolist() == expected_columns.type.tolist(), table_name
        assert actual_columns.character_maximum_length.fillna(0).tolist() == [
            int(x) for x in expected_columns.length.fillna(0).tolist()
        ], table_name
        assert actual_columns.class_name.fillna(0).tolist() == expected_columns["class"].fillna(0).tolist(), table_name
        assert actual_columns.xml_column_name.tolist() == expected_columns.xml_version.tolist(), table_name

    assert True


def regression_load(filename):
    def recode_excel_sheet_name(row):
        value = row['excel_sheet']
        if pd.notnull(value) and len(value) > 0 and value != 'nan':
            return value
        return row['table_name']

    tables: pd.DataFrame = pd.read_excel(
        filename,
        'Tables',
        dtype={'table_name': 'str', 'java_class': 'str', 'pk_name': 'str', 'excel_sheet': 'str', 'notes': 'str'},
    )

    columns: pd.DataFrame = pd.read_excel(
        filename,
        'Columns',
        dtype={
            'table_name': 'str',
            'column_name': 'str',
            'nullable': 'str',
            'type': 'str',
            'type2': 'str',
            'class': 'str',
        },
    )

    tables['table_name_index'] = tables['table_name']
    tables = tables.set_index('table_name_index')

    tables['excel_sheet'] = tables.apply(recode_excel_sheet_name, axis=1)

    primary_keys: pd.DataFrame = pd.merge(
        tables,
        columns,
        how='inner',
        left_on=['table_name', 'pk_name'],
        right_on=['table_name', 'column_name'],
    )[['table_name', 'column_name', 'java_class']]
    primary_keys.columns = ['table_name', 'column_name', 'class_name']

    foreign_keys: pd.DataFrame = pd.merge(
        columns,
        primary_keys,
        how='inner',
        left_on=['column_name', 'class'],
        right_on=['column_name', 'class_name'],
    )[['table_name_x', 'column_name', 'table_name_y', 'class_name']]
    foreign_keys = foreign_keys[foreign_keys.table_name_x != foreign_keys.table_name_y]

    foreign_keys_lookup: dict[str, bool] = {
        x: True for x in list(foreign_keys.table_name_x + '#' + foreign_keys.column_name)
    }

    primary_keys_lookup: dict[str, bool] = {x: True for x in tables.table_name + '#' + tables.pk_name}

    classname_cache: dict[str, dict] = tables.set_index('java_class')['table_name'].to_dict()

    return {
        'tables': tables,
        'columns': columns,
        'primary_keys': primary_keys,
        'foreign_keys': foreign_keys,
        'foreign_keys_lookup': foreign_keys_lookup,
        'primary_keys_lookup': primary_keys_lookup,
        'classname_cache': classname_cache,
    }
