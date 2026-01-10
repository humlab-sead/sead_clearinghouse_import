from dataclasses import asdict, dataclass, field
from fnmatch import fnmatch
from functools import cached_property
from typing import Any, Hashable, Iterator, Optional

import pandas as pd

from importer.configuration import ConfigValue

from .utility import camel_case_name, load_dataframe_from_postgres

# pylint: disable=no-member

DTYPE_MAPPING: dict[str, str] = {
    "uuid": "string",
    "smallint": "Int16",
    "integer": "Int32",
    "bigint": "Int64",
    "boolean": "boolean",
    "character varying": "string",
    "text": "string",
    # 'numeric': 'float64',  # Use 'object' if preserving precision with Decimal
    "timestamp without time zone": "datetime64[ns]",
    "timestamp with time zone": "datetime64[ns, UTC]",
    "date": "datetime64[ns]",
    "numrange": "object",
    "int4range": "object",
}


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
        return list(self.asdict().keys())

    def values(self) -> list[str]:
        return list(self.asdict().values())

    def asdict(self) -> dict[str, Any]:
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

    def __getitem__(self, key: str) -> Column | dict[str, Column]:
        if key == "columns":
            return self.columns
        return self.columns[key]

    def __iter__(self) -> Iterator[Column]:
        return iter(self.columns.values())

    def __len__(self) -> int:
        return len(self.columns)

    def get_column(self, column_name: str) -> Column | None:
        return self.columns.get(column_name)

    def keys(self) -> list[str]:
        return list(asdict(self).keys())

    def values(self) -> list[Column]:
        return list(asdict(self).values())

    def column_names(self, skip_nullable: bool = False) -> list[str]:
        return sorted(c.column_name for c in self.columns.values() if not (skip_nullable and c.is_nullable))

    def nullable_column_names(self) -> list[str]:
        return sorted(c.column_name for c in self.columns.values() if c.is_nullable)


from collections.abc import Iterator, KeysView, ValuesView, ItemsView


class SeadSchema:

    def __init__(self, tables: dict[str, Table], source_tables: pd.DataFrame, source_columns: pd.DataFrame) -> None:
        self._tables: dict[str, Table] = dict(tables)  # defensive copy
        self._tables_lookup: dict[str, Table] = (
            self._tables
            | {t.java_class: t for t in self._tables.values()}
            | {t.excel_sheet: t for t in self._tables.values() if t.excel_sheet and t.excel_sheet != t.table_name}
        )
        self.source_tables: pd.DataFrame = source_tables
        self.source_columns: pd.DataFrame = source_columns

    def __getitem__(self, key: str) -> Table:
        return self._tables[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._tables)

    def __len__(self) -> int:
        return len(self._tables)

    def __contains__(self, key: str) -> bool:
        return key in self._tables

    def keys(self) -> KeysView[str]:
        return self._tables.keys()

    def values(self) -> ValuesView[Table]:
        return self._tables.values()

    def items(self) -> ItemsView[str, Table]:
        return self._tables.items()

    def get(self, key: str, default: Table | None = None) -> Table | None:
        return self._tables.get(key, default)

    def get_table(self, table_name: str) -> Table | None:
        if table_name in self._tables_lookup:
            return self._tables_lookup[table_name]
        return None

    @cached_property
    def lookup_tables(self) -> list[Table]:
        return [t for t in self.values() if t.is_lookup]

    @cached_property
    def aliased_tables(self) -> list[Table]:
        return [t for t in self.values() if t.excel_sheet != t.table_name]

    @cached_property
    def table_name2excel_sheet(self) -> dict[str, str]:
        return {t: x.excel_sheet for t, x in self.items()}


class SeadSchemaFactory:

    def create(self, sead_tables: pd.DataFrame, sead_columns: pd.DataFrame) -> SeadSchema:
        """Build a SeadSchema from sead_tables and sead_columns dataframes."""

        # Group columns once: table_name -> dataframe of that table's columns
        cols_by_table = dict(tuple(sead_columns.groupby("table_name", sort=False)))
        tables: dict[str, Table] = {}

        # Each row becomes one table definition dict (kwargs for Table)
        for table_name, table_row in sead_tables.to_dict(orient="index").items():

            assert isinstance(table_name, str)

            table_properties: dict[str, Any] = table_row  # type: ignore[arg-type]
            cols_df = cols_by_table.get(table_name)
            if cols_df is None:
                columns: dict[str, Column] = {}
            else:
                rows = cols_df.to_dict(orient="records")
                columns = {str(row["column_name"]): Column(**row) for row in rows}  # type: ignore[arg-type]

            tables[str(table_name)] = Table(columns=columns, **table_properties)  # type: ignore[arg-type]

        return SeadSchema(tables=tables, source_tables=sead_tables, source_columns=sead_columns)


class MetadataService:
    """Service class to access metadata information"""

    def __init__(self, db_uri: str, ignore_columns: list[str] | None = None) -> None:
        self.db_uri: str = db_uri
        self.ignore_columns: list[str] = (
            ignore_columns or ConfigValue("options.ignore_columns", default=[]).resolve() or []
        )

    def get_sead_tables(self) -> pd.DataFrame:
        """Returns a dataframe of tables from SEAD with attributes."""
        sql: str = """
            select  table_name,
                    pk_name,
                    java_class,
                    excel_sheet,
                    is_lookup,
                    is_unknown
            from clearing_house.clearinghouse_import_tables
        """
        return self.load_sead_data(sql, ["table_name"])

    def get_sead_columns(self) -> pd.DataFrame:
        """Returns a dataframe of table columns from SEAD with attributes."""
        return self.load_sead_columns(self.ignore_columns)

    def get_sead_dtypes(self) -> dict[str, str]:
        """Returns a dict of table to datatype mappings."""
        sql: str = """
            select distinct column_name, data_type
            from sead_utility.table_columns where table_schema = 'public'
        """
        sead_types: dict[str, str] = self.load_sead_data(sql, index=["column_name"]).to_dict()["data_type"]

        dtypes: dict[str, str] = {k: DTYPE_MAPPING[v] for k, v in sead_types.items() if v in DTYPE_MAPPING}
        return dtypes

    # FIXME: Belongs in a data service class
    def get_primary_key_values(self, table_name: str, pk_name: str) -> set[int]:
        """Returns all unique primary keys for `table_name` in SEAD."""
        sql: str = f"""
            select distinct {pk_name}
            from {table_name}
        """
        keys: set = set(self.load_sead_data(sql, index=[pk_name]).index)
        return keys

    # FIXME: Belongs in a data service class
    def load_sead_data(
        self, sql: str | pd.DataFrame, index: list[str], sortby: list[str] | None = None
    ) -> pd.DataFrame:
        """Returns a dataframe of tables from SEAD with attributes."""
        index = index if isinstance(index, list) else [index]
        sortby = sortby if isinstance(sortby, list) else [sortby] if sortby else None
        data: pd.DataFrame = (
            (sql if isinstance(sql, pd.DataFrame) else load_dataframe_from_postgres(sql, self.db_uri, index_col=None))
            .set_index(index, drop=False)
            .rename_axis([f"index_{x}" for x in index])
            .sort_values(by=sortby if sortby else index)
        )
        return data

    def load_sead_columns(self, ignore_columns: list[str] | None = None) -> pd.DataFrame:
        """Returns a dataframe of table columns from SEAD with attributes."""
        sql: str = """
            select  xml_column_name
                    position
                    numeric_precision
                    numeric_scale
                    character_maximum_length
                    is_nullable
                    is_pk
                    is_fk
                    fk_table_name
                    fk_column_name
                    class_name
            from clearing_house.clearinghouse_import_columns
        """
        data: pd.DataFrame = self.load_sead_data(sql, ["table_name", "column_name"], ["table_name", "position"])
        if ignore_columns:
            columns_to_ignore: list[str] = [
                c for c in data["column_name"].unique() if any(fnmatch(c, pattern) for pattern in ignore_columns)
            ]
            data = data[~data["column_name"].isin(columns_to_ignore)]

        return data


class Metadata:
    """Logic related to Excel metadata file
    FIXME: The Excel metadata file is deprecated, this class should be removed in favor of SeadSchema and MetadataService
    """

    def __init__(self, db_uri: str, ignore_columns: list[str] | None = None) -> None:
        self.service: MetadataService = MetadataService(db_uri, ignore_columns)
        self.foreign_key_aliases: dict[str, str] = {"updated_dataset_id": "dataset_id"}

    @cached_property
    def sead_tables(self) -> pd.DataFrame:
        """Returns a dataframe of tables from SEAD with attributes."""
        return self.service.get_sead_tables()

    @cached_property
    def sead_columns(self) -> pd.DataFrame:
        """Returns a dataframe of table columns from SEAD with attributes."""
        return self.service.get_sead_columns()

    @cached_property
    def sead_dtypes(self) -> dict[str, str]:
        """Returns a dict of table to datatype mappings."""
        return self.service.get_sead_dtypes()

    @cached_property
    def sead_schema(self) -> SeadSchema:
        """Returns a dictionary of table attributes i.e. a row from sead_tables as a dictionary"""
        return SeadSchemaFactory().create(self.sead_tables, self.sead_columns)

    def __getitem__(self, what: str) -> Table | Column:
        table_name, column_name = what if isinstance(what, tuple) else (what, None)
        if column_name is not None:
            return self.get_column(table_name, column_name)
        return self.get_table(table_name)

    def get_table(self, table_name: str) -> Table:
        table: Table = self.sead_schema.get_table(table_name)
        if table is None:
            raise KeyError(f"Table {table_name} not found in metadata")
        return table

    def get_column(self, table_name: str, column_name: str) -> Column:
        table: Table | None = self.get_table(table_name)
        if table is None:
            raise KeyError(f"Table {table_name} not found in metadata")
        if column_name not in table.columns:
            raise KeyError(f"Column {column_name} not found in metadata for table {table_name}")
        return table.columns[column_name]

    def __contains__(self, table_name: str) -> bool:
        return table_name in self.sead_schema

    def is_fk(self, table_name: str, column_name: str) -> bool:
        if column_name in self.foreign_key_aliases:
            return True
        return self.get_column(table_name, column_name).is_fk

    def is_pk(self, table_name: str, column_name: str) -> bool:
        return self.get_column(table_name, column_name).is_pk

    @cached_property
    def _foreign_keys(self) -> pd.DataFrame:
        """Returns foreign key columns from SEAD columns (performance only)."""
        return self.sead_columns[self.sead_columns.is_fk][["table_name", "column_name", "fk_table_name", "class_name"]]

    def get_tablenames_referencing(self, table_name: str) -> list[str]:
        """Returns a list of tablenames referencing the given table"""
        return self._foreign_keys.loc[(self._foreign_keys.fk_table_name == table_name)]["table_name"].tolist()

    def get_primary_keys(self, table_name: str) -> set[int]:
        """Returns all unique primary keys for `table_name` in SEAD."""
        table: Table = self.get_table(table_name)
        if table.pk_name is None:
            return set()
        return self.service.get_primary_key_values(table_name, table.pk_name)
