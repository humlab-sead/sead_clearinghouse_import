import numpy as np
import pandas as pd

from importer.model.metadata import Metadata

from .submission import SubmissionData

TYPE_COMPATIBILITY_MATRIX = {
    ("integer", "float64"): True,
    ("timestamp with time zone", "float64"): False,
    ("text", "float64"): False,
    ("character varying", "float64"): False,
    ("numeric", "float64"): True,
    ("timestamp without time zone", "float64"): False,
    ("boolean", "float64"): False,
    ("date", "float64"): False,
    ("smallint", "float64"): True,
    ("integer", "object"): False,
    ("timestamp with time zone", "object"): True,
    ("text", "object"): True,
    ("character varying", "object"): True,
    ("numeric", "object"): False,
    ("timestamp without time zone", "object"): True,
    ("boolean", "object"): False,
    ("date", "object"): True,
    ("smallint", "object"): False,
    ("integer", "int64"): True,
    ("timestamp with time zone", "int64"): False,
    ("text", "int64"): False,
    ("character varying", "int64"): False,
    ("numeric", "int64"): True,
    ("timestamp without time zone", "int64"): False,
    ("boolean", "int64"): False,
    ("date", "int64"): False,
    ("smallint", "int64"): True,
    ("timestamp with time zone", "datetime64[ns]"): True,
    ("date", "datetime64[ns]"): True,
    #  ('character varying', 'datetime64[ns]'): True
}

NUMERIC_TYPES: list[str] = ["numeric", "integer", "smallint"]


class DataTableSpecification:
    """Specification class that tests validity of submission"""

    def __init__(self, metadata: Metadata, ignore_columns: list[str]) -> None:
        self.errors: list[str] = []
        self.warnings: list[str] = []
        self.ignore_columns: list[str] = ignore_columns or ["date_updated"]
        self.metadata: Metadata = metadata

    def is_satisfied_by(self, submission: SubmissionData) -> bool:
        self.errors = []
        self.warnings = []

        for table_name in submission.index_tablenames:
            self.is_satisfied_by_table(submission, table_name)

        return len(self.errors) == 0

    def is_satisfied_by_table(self, submission: SubmissionData, table_name: str) -> None:
        try:
            # Must exist as data table in metadata
            self.is_satisfied_by_table_must_exist_policy(submission, table_name)

            if not submission.exists(table_name):
                return

            data_table: pd.DataFrame = submission.data_tables[table_name]

            self.is_satisfied_by_system_id_policy(submission, table_name, data_table)
            self.is_satisfied_by_no_missing_columns_policy(submission, table_name)
            self.is_satisfied_by_has_pk_policy(table_name, data_table)
            self.is_satisfied_by_lookup_data_policy(submission, table_name)

            for _, field in self.metadata.sead_table_columns(
                submission, table_name, ignore_columns=self.ignore_columns
            ).iterrows():
                column: dict = field.to_dict()

                self.is_satisfied_by_type_match_policy(data_table, table_name, column)
                self.is_satisfied_by_is_numeric_policy(data_table, table_name, column)
                self.is_satisfied_by_id_is_fk_convention(table_name, column)

        except Exception as e:
            self.errors.append("CRITICAL ERROR occurred when validating {}: {}".format(table_name, str(e)))
            raise

    def is_satisfied_by_table_must_exist_policy(self, submission: SubmissionData, table_name) -> None:
        if submission.exists(table_name) and table_name not in submission.tables_with_data:
            # Check if it has an alias
            table_specification = self.metadata[table_name]
            alias_name = table_specification["excel_sheet"] or "no_alias"
            if alias_name not in submission.tables_with_data:
                """Not in submission table index sheet"""
                self.errors.append("CRITICAL ERROR Table {0} not defined as submission table".format(table_name))

        if not submission.exists(table_name):
            """No data sheet"""
            self.errors.append("CRITICAL ERROR {0} has NO DATA!".format(table_name))

    def is_satisfied_by_type_match_policy(
        self,
        data_table: pd.DataFrame,
        table_name: str,
        column_specification: dict[str, str],
    ) -> None:
        column_name: str = column_specification["column_name"]

        if column_name not in data_table.columns:
            return

        if len(data_table) == 0:
            """Cannot determine type if table is empty"""
            return

        data_column_type: str = data_table.dtypes[column_name].name
        if not TYPE_COMPATIBILITY_MATRIX.get((column_specification["type"], data_column_type), False):
            self.warnings.append(
                "WARNING type clash: {}.{} {}<=>{}".format(
                    table_name,
                    column_name,
                    column_specification["type"],
                    data_column_type,
                )
            )

    def is_satisfied_by_is_numeric_policy(self, data_table, table_name, column) -> None:
        if column["column_name"] not in data_table.columns:
            return

        if column["type"] not in NUMERIC_TYPES:
            return

        series: pd.Series = data_table[column["column_name"]]
        series = series[~series.isna()]
        ok_mask: pd.Series = series.apply(np.isreal)
        if not ok_mask.all():
            error_values = " ".join(list(set(series[~ok_mask])))[:200]
            self.errors.append(
                "CRITICAL ERROR Column {}.{} has non-numeric values: {}".format(
                    table_name, column["column_name"], error_values
                )
            )

    def is_satisfied_by_has_pk_policy(self, table_name: str, data_table: pd.DataFrame) -> None:
        primary_key_name: str = self.metadata[table_name]["pk_name"]

        if primary_key_name not in data_table.columns:
            self.errors.append('CRITICAL ERROR Table {} has no PK named "{}"'.format(table_name, primary_key_name))

    def is_satisfied_by_system_id_policy(
        self, submission: SubmissionData, table_name: str, data_table: pd.DataFrame
    ):  # pylint: disable=unused-argument
        # Must have a system identity
        # if not submission.has_system_id(table_name):
        if "system_id" not in data_table.columns:
            self.errors.append("{0} has no system id data column".format(table_name))
            return

        if data_table.system_id.isnull().values.any():
            self.errors.append("CRITICAL ERROR {0} has missing system id values".format(table_name))

        try:
            # duplicate_mask = data_table[~data_table.system_id.isna()].duplicated('system_id')
            duplicate_mask: pd.Series = data_table.duplicated("system_id")
            duplicates: list[int] = [int(x) for x in set(data_table[duplicate_mask].system_id)]
            if len(duplicates) > 0:
                error_values: str = " ".join([str(x) for x in duplicates])[:200]
                self.errors.append(
                    "CRITICAL ERROR Table {} has DUPLICATE system ids: {}".format(table_name, error_values)
                )
        except Exception as _:
            self.warnings.append("WARNING! Duplicate check of {}.{} failed".format(table_name, "system_id"))

    def is_satisfied_by_id_is_fk_convention(
        self,
        table_name: str,
        column_specification: dict[str, str],
    ) -> None:
        column_name: str = column_specification["column_name"]

        is_fk: bool = self.metadata.is_fk(table_name, column_name)
        is_pk: bool = self.metadata.is_pk(table_name, column_name)

        if column_name[-3:] == "_id" and not (is_fk or is_pk):
            self.warnings.append(
                'WARNING! Column {}.{}: ends with "_id" but NOT marked as PK/FK'.format(table_name, column_name)
            )

    def is_satisfied_by_no_missing_columns_policy(
        self,
        submission: SubmissionData,
        table_name: str,
    ) -> None:  # pylint: disable=unused-argument
        """All fields in metadata.Table.Fields MUST exist in DataTable.columns"""
        meta_column_names: list[str] = sorted(self.metadata[table_name]['columns'].keys())
        data_column_names: list[str] = (
            sorted(submission.data_tables[table_name].columns.values.tolist())
            if submission.exists(table_name) and table_name in self.metadata
            else []
        )

        missing_column_names = list(set(meta_column_names) - set(data_column_names) - set(self.ignore_columns))
        extra_column_names = list(
            set(data_column_names) - set(meta_column_names) - set(self.ignore_columns) - set(["system_id"])
        )

        if len(missing_column_names) > 0:
            self.errors.append(
                "ERROR {0} has MISSING DATA columns: ".format(table_name) + (", ".join(missing_column_names))
            )

        if len(extra_column_names) > 0:
            self.warnings.append(
                "WARNING {0} has EXTRA DATA columns: ".format(table_name) + (", ".join(extra_column_names))
            )

    def is_satisfied_by_lookup_data_policy(self, submission: SubmissionData, table_name: str) -> None:
        if not submission.exists(table_name):
            return

        if not self.metadata.is_lookup_table(table_name):
            return

        data_table: pd.DataFrame = submission.data_tables[table_name]
        pk_name: str = self.metadata[table_name]["pk_name"]

        if data_table[pk_name].isnull().any():
            self.errors.append("CRITICAL ERROR {} new values not allowed for lookup table.".format(table_name))
