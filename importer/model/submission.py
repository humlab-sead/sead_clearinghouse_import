import contextlib
import functools
from typing import Self

import numpy as np
import pandas as pd
from loguru import logger

from importer.utility import flatten_sets

from .metadata import Metadata


def load_excel_sheet(reader: pd.ExcelFile, sheetname: str) -> pd.DataFrame:
    with contextlib.suppress(Exception):
        return reader.parse(sheetname)


def load_excel(metadata: Metadata, source: str | pd.ExcelFile) -> Self:
    """Loads the submission file into a SubmissionData object"""
    reader: pd.ExcelFile = pd.ExcelFile(source) if isinstance(source, str) else source

    data_tables: dict[str, pd.DataFrame] = {
        x["table_name"]: load_excel_sheet(reader, x["excel_sheet"]) for i, x in metadata.sead_tables.iterrows()
    }

    logger.info(f"read sheets: {','.join(k for k,v in data_tables.items() if v is not None)}")
    logger.info(f"missing sheets: {','.join(k for k,v in data_tables.items() if v is None) or 'none'}")

    data_table_index: pd.DataFrame = load_excel_sheet(reader, "data_table_index")

    if data_table_index is None:
        logger.exception("submission file has no data_table_index")

    reader.close()

    return SubmissionData(data_tables, data_table_index)


class SubmissionData:
    """Logic dealing with the submission data"""

    def __init__(self, data_tables: dict[str, pd.DataFrame], data_table_index: pd.DataFrame) -> None:
        self.data_tables: dict[str, pd.DataFrame] = data_tables
        self.data_table_index: pd.DataFrame = data_table_index

    def __getitem__(self, key: str) -> pd.DataFrame:
        return self.data_tables[key]

    def __contains__(self, key: str) -> bool:
        return self.exists(key)

    def exists(self, table_name: str) -> bool:
        return table_name in self.data_tables.keys() and self[table_name] is not None

    def has_system_id(self, table_name: str) -> bool:
        return self.exists(table_name) and "system_id" in self[table_name].columns

    @property
    def data_tablenames(self) -> list[str]:
        return [x for x in self.data_tables.keys() if self.exists(x)]

    @property
    def index_tablenames(self) -> list[str]:
        return self.data_table_index["table_name"].tolist()

    def get_referenced_keyset(self, metadata: Metadata, table_name: str) -> list[str]:
        """Returns a set of keys from the referenced table"""
        pk_name: str = metadata[table_name]["pk_name"]
        if pk_name is None:
            return []
        ref_tablenames: list[str] = metadata.get_tablenames_referencing(table_name)
        sets_of_keys: list[set] = [
            set(
                self.data_tables[foreign_name][pk_name].loc[~np.isnan(self.data_tables[foreign_name][pk_name])].tolist()
            )
            for foreign_name in ref_tablenames
            if not self.data_tables[foreign_name] is None
        ]
        return functools.reduce(flatten_sets, sets_of_keys or [], [])
