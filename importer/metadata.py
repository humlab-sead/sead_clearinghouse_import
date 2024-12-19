from dataclasses import asdict, dataclass, field
from functools import cached_property
from typing import Any

import pandas as pd

from importer.configuration.inject import ConfigValue

from .utility import camel_case_name, load_sead_data, load_sead_columns

# pylint: disable=no-member


@dataclass
class Column:
    table_name: str
    column_name: str
    xml_column_name: str
    position: int
    data_type: str
    numeric_precision: int
    numeric_scale: int
    character_maximum_length: int
    is_nullable: bool
    is_pk: bool
    is_fk: bool
    fk_table_name: str | None
    fk_column_name: str | None
    class_name: str

    def __contains__(self, key: str) -> bool:
        return key in self.__dict__

    def __getitem__(self, key: str) -> Any:
        return self.__dict__[key]

    def keys(self) -> list[str]:
        return self.asdict().keys()

    def values(self) -> list[str]:
        return self.asdict().values()

    def asdict(self) -> list[str]:
        return asdict(self)

    @property
    def camel_case_column_name(self) -> str:
        return camel_case_name(self.column_name)


@dataclass
class Table:
    table_name: str
    pk_name: str
    java_class: str
    excel_sheet: str
    is_lookup: bool
    is_new: bool = field(default=False)
    is_unknown: bool = field(default=False)
    columns: dict[str, Column] = field(default_factory=dict)

    def __contains__(self, key: str) -> bool:
        return key in self.columns

    def __getitem__(self, key: str) -> Column:
        if key == "columns":
            return self.columns
        return self.columns[key]

    def __iter__(self) -> Column:
        return iter(self.columns.values())

    def __len__(self) -> int:
        return len(self.columns)

    def keys(self) -> list[str]:
        return asdict(self).keys()

    def values(self) -> list[Column]:
        return asdict(self).values()

    def column_names(self, skip_nullable: bool = False) -> list[str]:
        return sorted(c.column_name for c in self.columns.values() if not (skip_nullable and c.is_nullable))

    def nullable_column_names(self) -> list[str]:
        return sorted(c.column_name for c in self.columns.values() if c.is_nullable)


class SeadSchema(dict[str, Table]):
    @cached_property
    def sead_schema_by_class(self) -> "SeadSchema":
        return SeadSchema({t.java_class: t for t in self.values()})

    def get_table_spec(self, table_name: str) -> Table:
        return self.get(table_name) if table_name in self else self.sead_schema_by_class.get(table_name)

    @cached_property
    def lookup_tables(self) -> list[Table]:
        return [t for t in self.values() if t.is_lookup]


class Metadata:
    """Logic related to Excel metadata file"""

    def __init__(self, db_uri: str, ignore_columns: list[str] = None) -> None:
        self.db_uri: str = db_uri
        self.foreign_key_aliases: dict[str, str] = {"updated_dataset_id": "dataset_id"}
        self.ignore_columns: list[str] = ignore_columns or ConfigValue("options.ignore_columns", default=[]).resolve()

    @cached_property
    def sead_tables(self) -> pd.DataFrame:
        """Returns a dataframe of tables from SEAD with attributes."""
        sql: str = "select * from clearing_house.clearinghouse_import_tables"
        return load_sead_data(self.db_uri, sql, ["table_name"])

    @cached_property
    def sead_columns(self) -> pd.DataFrame:
        """Returns a dataframe of table columns from SEAD with attributes."""
        return load_sead_columns(self.db_uri, self.ignore_columns)

    @cached_property
    def sead_schema(self) -> SeadSchema:
        """Returns a dictionary of table attributes i.e. a row from sead_tables as a dictionary"""

        def get_column_spec(table_name: str) -> Column:
            return {
                k: Column(**v)
                for k, v in self.sead_columns[self.sead_columns.table_name == table_name]
                .set_index('column_name', drop=False)
                .to_dict(orient='index')
                .items()
            }

        schema: SeadSchema = SeadSchema(
            {k: Table(columns=get_column_spec(k), **v) for k, v in self.sead_tables.to_dict(orient='index').items()}
        )
        return schema

    def __getitem__(self, what: str) -> Table | Column:
        table_name, column_name = what if isinstance(what, tuple) else (what, None)
        table: Table = self.sead_schema.get_table_spec(table_name)
        if table is None:
            raise KeyError(f"Table {table_name} not found in metadata")
        if column_name is not None:
            if column_name not in table.columns:
                raise KeyError(f"Column {column_name} not found in metadata for table {table_name}")
            return table.columns[column_name]
        return table

    def __contains__(self, table_name: str) -> bool:
        return table_name in self.sead_schema

    def is_fk(self, table_name: str, column_name: str) -> bool:
        if column_name in self.foreign_key_aliases:
            return True
        return self[table_name, column_name].is_fk

    def is_pk(self, table_name: str, column_name: str) -> bool:
        return self[table_name, column_name].is_pk

    @cached_property
    def foreign_keys(self) -> pd.DataFrame:
        """Returns foreign key columns from SEAD columns (performance only)."""
        return self.sead_columns[self.sead_columns.is_fk][['table_name', 'column_name', 'fk_table_name', 'class_name']]

    def get_tablenames_referencing(self, table_name: str) -> list[str]:
        """Returns a list of tablenames referencing the given table"""
        return self.foreign_keys.loc[(self.foreign_keys.fk_table_name == table_name)]["table_name"].tolist()
