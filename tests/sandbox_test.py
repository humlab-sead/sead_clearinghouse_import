import json
from os.path import isfile

import pandas as pd
import pytest

from importer.configuration.inject import ConfigValue
from importer.metadata import Metadata
from tests.utility import get_db_uri, load_excel_by_regression

# @pytest.mark.skip(reason="sandbox test")
# def test_download_sead_comments():
#     """Stores SEAD comments in a markdown file"""

#     uri: str = get_db_uri()
#     sql = "select * from sead_utility.sead_comments2"
#     df = pd.read_sql(sql, uri)
#     # df.to_excel('sead_comments_20240201.xlsx', index=False)
#     table_template: str = """
# # {{table['table_name']}}
# {{table['comment'] or ''}}
# {% for column in columns -%}
# ## {{table['table_name']}}.{{column['column_name']}} {{'PK' if column['is_pk'] == 'YES' else ''}} {{'FK' if column['is_fk'] == 'YES' else ''}}
# {{column['comment'] or ''}}
# {%- endfor %}"""

#     jinja_env: Environment = Environment()
#     template: Template = jinja_env.from_string(table_template)

#     with open('sead_comments_20240201.md', 'w') as f:
#         for table_name in df.table_name.unique():
#             records: list[dict] = df[df.table_name == table_name].to_dict('records')
#             table: dict = next(x for x in records if x['column_name'] is None)
#             columns: list[dict] = [x for x in records if x['column_name'] is not None]
#             md_str: str = template.render(table=table, columns=columns)
#             f.write(md_str)


@pytest.mark.skipif(isfile('tests/test_data/sead_columns.json'), reason='Used for generating test data only')
def test_load_metadata_from_postgres():
    """Use this test to store SEAD metadata in json files for regression testing"""
    metadata: Metadata = Metadata(get_db_uri())
    test_tables: list[str] = ConfigValue("test:tables").resolve()
    with open('tests/test_data/sead_tables.json', 'w') as outfile:
        data: dict = metadata.sead_tables[metadata.sead_tables.table_name.isin(test_tables)].to_dict('records')
        json.dump(data, outfile, indent=4)

    with open('tests/test_data/sead_columns.json', 'w') as outfile:
        data: dict = metadata.sead_columns.fillna(0)[metadata.sead_columns.table_name.isin(test_tables)].to_dict(
            'records'
        )
        json.dump(data, outfile, indent=4)

    assert isinstance(metadata, Metadata)
    assert isinstance(metadata.sead_tables, pd.DataFrame)
    assert isinstance(metadata.sead_columns, pd.DataFrame)
    assert isinstance(metadata.sead_tables, pd.DataFrame)
    assert isinstance(metadata.sead_schema, dict)


###### REMOVE CODE BELOW WHEN DONE
@pytest.mark.skipif(not isfile('data/metadata_20231223.xlsx'), reason='metadata_20231223.xlsx not found')
def test_load_by_sql_is_same_as_load_by_excel(metadata: Metadata):
    excel_columns: pd.DataFrame = load_excel_by_regression('data/metadata_20231223.xlsx').get('columns')
    test_tables: list[str] = ConfigValue("test:tables").resolve()
    excel_columns = excel_columns[excel_columns.table_name.isin(test_tables)]

    assert isinstance(metadata.sead_columns, pd.DataFrame)

    assert len(metadata.sead_columns.shape) == len(excel_columns.shape)

    for table_name in test_tables:
        # for table_name in ['tbl_dendro_dates']: #test_tables:

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
