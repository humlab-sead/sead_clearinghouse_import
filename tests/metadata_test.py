import json
from os.path import isfile
from unittest.mock import patch

import pandas as pd
import pytest

import importer
from importer.model.metadata import Metadata
from importer.utility import dburi_from_env  # Assuming Metadata is the class name

# pylint: disable=redefined-outer-name,no-member

TEST_TABLES: list[str] = [
    'tbl_sites',
    'tbl_locations',
    'tbl_sample_group_sampling_contexts',
    'tbl_record_types',
]


def patch_load_tables_dataframe(*args, **kwargs) -> pd.DataFrame:  # pylint: disable=unused-argument
    return pd.read_json('tests/test_data/sead_tables.json').set_index("table_name", drop=False)


def patch_load_columns_dataframe(*args, **kwargs) -> pd.DataFrame:  # pylint: disable=unused-argument
    return pd.read_json('tests/test_data/sead_columns.json').set_index("table_name", drop=False)


@pytest.fixture
@patch('importer.model.metadata.load_dataframe_from_postgres', patch_load_columns_dataframe)
@patch(
    "importer.model.metadata.load_dataframe_from_postgres",
    patch_load_tables_dataframe,
)
def metadata() -> Metadata:
    instance = importer.model.metadata.Metadata("a-dummy-db-uri")
    instance.__dict__['sead_tables'] = patch_load_tables_dataframe()
    instance.__dict__['sead_columns'] = patch_load_columns_dataframe()
    return instance


def test_load_metadata_from_postgres():
    metadata: Metadata = Metadata(dburi_from_env())

    with open('tests/test_data/sead_tables.json', 'w') as outfile:
        data: dict = metadata.sead_tables[metadata.sead_tables.table_name.isin(TEST_TABLES)].to_dict('records')
        json.dump(data, outfile, indent=4)

    with open('tests/test_data/sead_columns.json', 'w') as outfile:
        data: dict = metadata.sead_columns.fillna(0)[metadata.sead_columns.table_name.isin(TEST_TABLES)].to_dict('records')
        json.dump(data, outfile, indent=4)

    assert isinstance(metadata, Metadata)

    assert isinstance(metadata.sead_tables, pd.DataFrame)

    assert isinstance(metadata.sead_columns, pd.DataFrame)
    assert isinstance(metadata.sead_tables, pd.DataFrame)
    assert isinstance(metadata.sead_table_specifications, dict)


def test_load_sql_from_file():
    sql: str = Metadata.load_sql_from_file("sead_tables")
    assert "select" in sql.lower()
    sql: str = Metadata.load_sql_from_file("sead_columns")
    assert "select" in sql.lower()


def test_tables_specifications(metadata: Metadata):
    assert isinstance(metadata.sead_table_specifications, dict)

    assert len(metadata.sead_tables) == len(metadata.sead_table_specifications)
    assert set(metadata.sead_tables.columns) | {'columns'} == set(metadata.sead_table_specifications['tbl_sites'].keys())

    assert 'columns' in metadata.sead_table_specifications['tbl_sites'].keys()
    assert 'site_id' in metadata.sead_table_specifications['tbl_sites']['columns'].keys()
    assert len(metadata.sead_table_specifications['tbl_sites']['columns']) == len(
        metadata.sead_columns[metadata.sead_columns.table_name == 'tbl_sites']
    )
    assert metadata.sead_table_specifications['tbl_sites']['columns']['site_id']['is_pk'] is True
    assert metadata.sead_table_specifications['tbl_sites']['columns']['site_name']['is_pk'] is False
    assert metadata.sead_table_specifications['tbl_locations']['columns']['location_type_id']['is_fk'] is True
    assert metadata.sead_table_specifications['tbl_locations']['columns']['location_id']['is_fk'] is False
    assert metadata.sead_table_specifications['tbl_locations']['columns']['location_id']['is_pk'] is True


###### REMOVE CODE BELOW WHEN DONE
@pytest.mark.skipif(not isfile('data/metadata_20231223.xlsx'), reason='metadata_20231223.xlsx not found')
def test_load_by_sql_is_same_as_load_by_excel(metadata: Metadata):
    expected_columns: pd.DataFrame = regression_load('data/metadata_20231223.xlsx')['columns']
    expected_columns = expected_columns[expected_columns.table_name.isin(TEST_TABLES)]

    assert isinstance(metadata.sead_columns, pd.DataFrame)

    assert len(metadata.sead_columns.shape) == len(expected_columns.shape)

    assert metadata.sead_columns['table_name'].tolist() == expected_columns['table_name'].tolist()
    assert metadata.sead_columns['column_name'].tolist() == expected_columns['column_name'].tolist()
    assert metadata.sead_columns['position'].tolist() == expected_columns['position'].tolist()
    assert metadata.sead_columns['is_nullable'].tolist() == expected_columns['nullable'].tolist()
    assert metadata.sead_columns['data_type'].tolist() == expected_columns['type'].tolist()
    assert metadata.sead_columns['character_maximum_length'].fillna(0).tolist() == expected_columns['length'].fillna(0).tolist()
    assert metadata.sead_columns['f_class_name'].fillna(0).tolist() == expected_columns['class'].fillna(0).tolist()
    assert metadata.sead_columns['xml_column_name'].tolist() == expected_columns['xml_version'].tolist()

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
        dtype={'table_name': 'str', 'column_name': 'str', 'nullable': 'str', 'type': 'str', 'type2': 'str', 'class': 'str'},
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
    )[['table_name_x', 'table_name_y', 'column_name', 'class_name']]
    foreign_keys = foreign_keys[foreign_keys.table_name_x != foreign_keys.table_name_y]

    foreign_keys_lookup: dict[str, bool] = {x: True for x in list(foreign_keys.table_name_x + '#' + foreign_keys.column_name)}

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
