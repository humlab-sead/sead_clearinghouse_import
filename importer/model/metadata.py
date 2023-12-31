from functools import cached_property
from typing import Any

import pandas as pd

from importer.utility import load_sead_data

# pylint: disable=no-member


class Metadata:
    """Logic related to Excel metadata file"""

    def __init__(self, db_uri: str) -> None:
        self.db_uri: str = db_uri
        self.foreign_key_aliases: dict[str, str] = {"updated_dataset_id": "dataset_id"}

    @cached_property
    def sead_tables(self) -> pd.DataFrame:
        """Returns a dataframe of tables from SEAD with attributes."""
        return load_sead_data(self.db_uri, "sead_tables", ["table_name"])

    @cached_property
    def sead_columns(self) -> pd.DataFrame:
        """Returns a dataframe of table columns from SEAD with attributes."""
        return load_sead_data(self.db_uri, "sead_columns", ["table_name", "column_name"], ["table_name", "position"])

    @cached_property
    def sead_schema(self) -> dict[str, Any]:
        """Returns a dictionary of table attributes i.e. a row from sead_tables as a dictionary"""
        schema: dict[str, Any] = self.sead_tables.to_dict(orient='index')
        for k, v in schema.items():
            v['columns'] = (
                self.sead_columns[self.sead_columns.table_name == k]
                .set_index('column_name', drop=False)
                .to_dict(orient='index')
            )
        return schema

    def __getitem__(self, what: str) -> dict[str, Any]:
        table_name, column_name = what if isinstance(what, tuple) else (what, None)
        if table_name not in self.sead_schema:
            raise KeyError(f"Table {table_name} not found in metadata")
        table: dict[str, Any] = self.sead_schema[table_name]
        if column_name is not None:
            if column_name not in table['columns']:
                raise KeyError(f"Column {column_name} not found in metadata for table {table_name}")
            return table['columns'][column_name]
        return table

    def __contains__(self, table_name: str) -> bool:
        return table_name in self.sead_schema

    def is_fk(self, table_name: str, column_name: str) -> bool:
        if column_name in self.foreign_key_aliases:
            return True
        return self[table_name, column_name]["is_fk"]

    def is_pk(self, table_name: str, column_name: str) -> bool:
        return self[table_name, column_name]['is_pk']

    @cached_property
    def foreign_keys(self) -> pd.DataFrame:
        """Returns foreign key columns from SEAD columns (performance only)."""
        return self.sead_columns[self.sead_columns.is_fk][['table_name', 'column_name', 'f_table_name', 'class_name']]

    def get_tablenames_referencing(self, table_name: str) -> list[str]:
        """Returns a list of tablenames referencing the given table"""
        return self.foreign_keys.loc[(self.foreign_keys.f_table_name == table_name)]["table_name"].tolist()

