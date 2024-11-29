import contextlib
import functools

import pandas as pd
from loguru import logger

from .metadata import Metadata, SeadSchema, Table
from .utility import flatten_sets, log_decorator


def load_excel_sheet(reader: pd.ExcelFile, sheetname: str) -> pd.DataFrame:
    with contextlib.suppress(Exception):
        return reader.parse(sheetname)


@log_decorator(enter_message=' --> loading excel...', exit_message=' --> done loading excel')
def load_excel(*, metadata: Metadata, source: str | pd.ExcelFile) -> "SubmissionData":
    """Loads the submission file into a SubmissionData object"""

    schema: SeadSchema = metadata.sead_schema
    data_table_index: pd.DataFrame = None

    with pd.ExcelFile(source) if isinstance(source, str) else source as reader:

        data_tables: dict[str, pd.DataFrame] = {
            tablename: load_excel_sheet(reader, table.excel_sheet)
            for tablename, table in schema.items()
            if table.excel_sheet in reader.sheet_names
        }

        logger.info(f"   read sheets: {','.join(k for k in data_tables)}")
        logger.info(f"ignored sheets: {','.join(set(reader.sheet_names) - set(data_tables.keys()))}")

        if 'data_table_index' in reader.sheet_names:
            data_table_index: pd.DataFrame = load_excel_sheet(reader, "data_table_index")
            logger.info("using data_table_index found in Excel")

    return SubmissionData(data_tables, metadata, data_table_index)


class SubmissionData:
    """Logic dealing with the submission data"""

    def __init__(
        self, data_tables: dict[str, pd.DataFrame], metadata: Metadata, data_table_index: pd.DataFrame
    ) -> None:
        self.data_tables: dict[str, pd.DataFrame] = data_tables
        self.metadata: Metadata = metadata
        self.data_table_index: pd.DataFrame = (
            data_table_index
            if data_table_index is not None
            else metadata.sead_tables[metadata.sead_tables.table_name.isin(data_tables.keys())]
        )

    def __getitem__(self, key: str) -> pd.DataFrame:
        return self.data_tables[key] if key in self.data_tables else None

    def __contains__(self, key: str) -> bool:
        if key in self.data_tables:
            return True
        if key in self.metadata:
            return self.metadata[key].excel_sheet in self.data_tables
        return False

    def has_system_id(self, table_name: str) -> bool:
        return table_name in self.data_tables and "system_id" in self[table_name].columns

    def has_pk_id(self, table_name: str) -> bool:
        return self.metadata[table_name].pk_name in self[table_name].columns

    def has_new_data(self, table_name: str) -> bool:
        pk_name: str = self.metadata[table_name].pk_name
        if not self.has_pk_id(table_name):
            raise ValueError(f"Table {table_name}: PK column {pk_name} not found in submission")
        return any(self[table_name][pk_name].isnull())

    @property
    def data_table_names(self) -> list[str]:
        """Returns a list of all table names included in the submission"""
        return list(self.data_tables.keys())

    def get_referenced_keyset(self, metadata: Metadata, table_name: str) -> set[str]:
        """Returns all unique system ids in `table_name` that are referenced by any foreign key in any other table.
        NOTE: This function assumes PK and FK names are the same."""
        pk_name: str = metadata[table_name].pk_name
        if pk_name is None:
            return []
        fk_tables: list[str] = metadata.get_tablenames_referencing(table_name)
        referenced_pk_ids: list[set] = [
            set(self.data_tables[foreign_name][pk_name].loc[~self.data_tables[foreign_name][pk_name].isnull()].tolist())
            for foreign_name in fk_tables
            if not self.data_tables[foreign_name] is None
        ]
        return set(int(x) for x in functools.reduce(flatten_sets, referenced_pk_ids or [], []))
