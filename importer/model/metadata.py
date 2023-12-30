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
    def sead_table_specifications(self) -> dict[str, Any]:
        """Returns a dictionary of table attributes i.e. a row from sead_tables as a dictionary"""
        specification: dict[str, Any] = self.sead_tables.to_dict(orient='index')
        for k, v in specification.items():
            v['columns'] = self.sead_columns[self.sead_columns.table_name == k].set_index('column_name', drop=False).to_dict(orient='index')
        return specification

    @cached_property
    def primary_keys(self) -> pd.DataFrame:
        primary_keys: pd.DataFrame = pd.merge(
            self.sead_tables,
            self.sead_columns,
            how="inner",
            left_on=["table_name", "pk_name"],
            right_on=["table_name", "column_name"],
        )[["table_name", "column_name", "java_class"]]
        primary_keys.columns = ["table_name", "column_name", "class_name"]
        return primary_keys

    @cached_property
    def foreign_keys(self) -> pd.DataFrame:
        foreign_keys: pd.DataFrame = pd.merge(
            self.sead_columns,
            self.primary_keys,
            how="inner",
            left_on=["column_name", "class"],
            right_on=["column_name", "class_name"],
        )[["table_name", "column_name", "referenced_table_name", "class_name"]]
        foreign_keys.columns = ["table_name", "column_name", "referenced_table_name", "class_name"]
        foreign_keys = foreign_keys[foreign_keys.table_name != foreign_keys.referenced_table_name]
        return foreign_keys

    @cached_property
    def foreign_keys_lookup(self) -> set[str]:
        return {x for x in list(self.foreign_keys.table_name + "#" + self.foreign_keys.column_name)}

    @cached_property
    def primary_keys_lookup(self) -> set[str]:
        return {x for x in self.sead_tables.table_name + "#" + self.sead_tables.pk_name}

    @cached_property
    def class_name_lookup(self) -> dict[str, str]:
        return self.sead_tables.set_index("java_class")["table_name"].to_dict()

    def __getitem__(self, table_name: str) -> dict[str, Any]:
        if table_name not in self.sead_table_specifications:
            raise KeyError("Table {} not found in metadata".format(table_name))
        return self.sead_table_specifications[table_name]

    def __contains__(self, table_name: str) -> bool:
        return table_name in self.sead_table_specifications

    def is_fk(self, table_name: str, column_name: str) -> bool:
        if column_name in self.foreign_key_aliases:
            return True
        return self.sead_table_specifications[table_name][column_name]["is_fk"]

    def is_pk(self, table_name: str, column_name: str) -> bool:
        return self.sead_table_specifications[table_name][column_name]["is_pk"]

    def get_tablenames_referencing(self, table_name: str) -> list[str]:
        return self.foreign_keys.loc[(self.foreign_keys.referenced_table_name == table_name)]["table_name"].tolist()

    def is_lookup_table(self, table_name: str) -> bool:
        return self.sead_table_specifications[table_name]["is_lookup_table"]

    def table_fields(self, table_name: str) -> pd.DataFrame:
        return self.sead_columns[(self.sead_columns.table_name == table_name)]

    def sead_table_columns(self, table_name: str, ignore_columns: list[str] = None) -> pd.DataFrame:
        columns: pd.DataFrame = self.sead_columns[(self.sead_columns.table_name == table_name)]
        if ignore_columns is not None:
            columns = columns.loc[(~columns.column_name.isin(ignore_columns))]
        return columns
