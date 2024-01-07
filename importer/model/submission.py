import contextlib
import functools

import pandas as pd
from loguru import logger

from importer.utility import flatten_sets, log_decorator

from .metadata import Metadata


def load_excel_sheet(reader: pd.ExcelFile, sheetname: str) -> pd.DataFrame:
    with contextlib.suppress(Exception):
        return reader.parse(sheetname)


@log_decorator(enter_message=' --> loading excel...', exit_message=' --> done loading excel')
def load_excel(*, metadata: Metadata, source: str | pd.ExcelFile) -> "SubmissionData":
    """Loads the submission file into a SubmissionData object"""
    reader: pd.ExcelFile = pd.ExcelFile(source) if isinstance(source, str) else source

    data_tables: dict[str, pd.DataFrame] = {
        x["table_name"]: load_excel_sheet(reader, x["excel_sheet"]) for i, x in metadata.sead_tables.iterrows()
    }

    logger.info(f"read sheets: {','.join(k for k,v in data_tables.items() if v is not None)}")
    # logger.info(f"missing sheets: {','.join(k for k,v in data_tables.items() if v is None) or 'none'}")

    data_table_index: pd.DataFrame = load_excel_sheet(reader, "data_table_index")
    if data_table_index is None:
        logger.exception("submission file has no data_table_index")
    else:
        data_table_index['only_new_data'] = data_table_index['only_new_data'] == "YES"
        data_table_index['new_data'] = data_table_index['new_data'] == "YES"

    reader.close()

    return SubmissionData(data_tables, data_table_index)


class SubmissionData:
    """Logic dealing with the submission data"""

    def __init__(self, data_tables: dict[str, pd.DataFrame], data_table_index: pd.DataFrame) -> None:
        self.data_tables: dict[str, pd.DataFrame] = data_tables
        self.data_table_index: pd.DataFrame = data_table_index

    def __getitem__(self, key: str) -> pd.DataFrame:
        return self.data_tables[key] if key in self.data_tables else None

    def __contains__(self, key: str) -> bool:
        return self.exists(key)

    def exists(self, table_name: str) -> bool:
        return table_name in self.data_tables.keys() and self[table_name] is not None

    def has_system_id(self, table_name: str) -> bool:
        return self.exists(table_name) and "system_id" in self[table_name].columns

    @property
    def data_table_names(self) -> list[str]:
        return [x for x in self.data_tables.keys() if self.exists(x)]

    @property
    def index_table_names(self) -> list[str]:
        return self.data_table_index["table_name"].tolist()

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
        return set(functools.reduce(flatten_sets, referenced_pk_ids or [], []))
