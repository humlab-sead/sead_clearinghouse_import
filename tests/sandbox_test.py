import json
from os.path import isfile

import pandas as pd
import pytest

from importer.configuration.config import Config
from importer.metadata import Metadata
from importer.utility import create_db_uri

# @pytest.mark.skip(reason="sandbox test")
# def test_download_sead_comments():
#     """Stores SEAD comments in a markdown file"""

#     uri: str = create_db_uri(**cfg.get("options:database"))
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
def test_load_metadata_from_postgres(cfg: Config):
    """Use this test to store SEAD metadata in json files for regression testing"""
    metadata: Metadata = Metadata(create_db_uri(**cfg.get("options:database")))
    test_tables: list[str] = cfg.get("test:tables")
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
