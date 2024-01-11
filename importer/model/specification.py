import abc
import logging
from dataclasses import dataclass, field

import numpy as np
import pandas as pd
from loguru import logger

from ..utility import Registry, log_decorator
from .metadata import Metadata, TableSpec
from .submission import SubmissionData


class SpecificationRegistry(Registry):
    ...


@dataclass
class SpecificationMessages:
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


class SpecificationError(Exception):
    def __init__(self, messages: SpecificationMessages) -> None:
        super().__init__("Submission specification failed")
        self.messages: SpecificationMessages = messages


class SpecificationBase(abc.ABC):
    def __init__(self, metadata: Metadata, messages: SpecificationMessages, ignore_columns: list[str]) -> None:
        self.metadata: Metadata = metadata
        self.messages: SpecificationMessages = messages
        self.ignore_columns: list[str] = ignore_columns or ["date_updated"]

    @property
    def errors(self) -> list[str]:
        return self.messages.errors

    @property
    def warnings(self) -> list[str]:
        return self.messages.warnings

    def clear(self) -> None:
        self.messages.errors = []
        self.messages.warnings = []

    @abc.abstractmethod
    def is_satisfied_by(self, submission: SubmissionData, table_name: str) -> None:
        ...


class SubmissionSpecification(SpecificationBase):
    """Specification class that tests validity of submission"""

    def __init__(
        self,
        metadata: Metadata,
        *,
        messages: SpecificationMessages = None,
        ignore_columns: list[str] = None,
        raise_errors: bool = True,
    ) -> None:
        super().__init__(metadata, messages or SpecificationMessages(), ignore_columns)
        self.raise_errors: bool = raise_errors

    @log_decorator(enter_message=" ---> checking submission...", exit_message=" ---> submission checked")
    def is_satisfied_by(self, submission: SubmissionData, _: str = None) -> bool:
        """
        Check if the given submission satisfies all the specifications defined in the SpecificationRegistry.

        Parameters:
            submission (SubmissionData): The submission data to be checked.
            _ (str, optional): Ignored argument. Defaults to None.

        Returns:
            bool: True if all the specifications are satisfied, False otherwise.
        """
        self.clear()
        for cls in SpecificationRegistry.items.values():
            specification: SpecificationBase = cls(
                self.metadata, messages=self.messages, ignore_columns=self.ignore_columns
            )
            for table_name in submission.index_table_names:
                specification.is_satisfied_by(submission, table_name)

        self.log_messages(self.messages.warnings, logging.WARNING)
        self.log_messages(self.messages.errors, logging.ERROR)

        if self.raise_errors and len(specification.errors) > 0:
            raise SpecificationError(self.messages)

        return len(self.errors) == 0

    def log_messages(self, messages: list[str], level: int) -> None:
        if len(messages) > 0:
            for message in messages:
                try:
                    logger.log(level, message)
                except UnicodeEncodeError as ex:
                    logger.warning("WARNING! Failed to output warning message")
                    logger.exception(ex)


@SpecificationRegistry.register()
class SubmissionTableExistsSpecification(SpecificationBase):
    """Specification class that tests if table exists in submission"""

    def is_satisfied_by(self, submission: SubmissionData, table_name: str) -> None:
        if submission.exists(table_name) and table_name not in submission.data_table_names:
            # Check if it has an alias
            table_spec: TableSpec = self.metadata[table_name]
            alias_name: str = table_spec.excel_sheet or "no_alias"
            if alias_name not in submission.data_table_names:
                """Not in submission table index sheet"""
                self.errors.append(f"ERROR Table {table_name} not defined as submission table")

        if not submission.exists(table_name):
            """No data sheet"""
            self.errors.append(f"ERROR {table_name} has NO DATA!")


@SpecificationRegistry.register()
class ColumnTypesSpecification(SpecificationBase):
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

    def is_satisfied_by(self, submission: SubmissionData, table_name: str) -> None:
        data_table: pd.DataFrame = submission.data_tables[table_name]
        if len(data_table) == 0:
            """Cannot determine type if table is empty"""
            return

        for _, column_spec in self.metadata[table_name].columns.items():
            if column_spec.column_name not in data_table.columns:
                continue
            if column_spec.column_name in self.ignore_columns:
                continue
            data_column_type: str = data_table.dtypes[column_spec.column_name].name
            if all(data_table[column_spec.column_name].isna()):
                continue
            if not self.TYPE_COMPATIBILITY_MATRIX.get((column_spec.data_type, data_column_type), False):
                self.warnings.append(
                    f"WARNING type clash: {table_name}.{column_spec.column_name} {column_spec.data_type}<=>{data_column_type}"
                )


@SpecificationRegistry.register()
class SubmissionTableTypesSpecification(SpecificationBase):
    NUMERIC_TYPES: list[str] = ["numeric", "integer", "smallint"]

    def is_satisfied_by(self, submission: SubmissionData, table_name: str) -> None:
        data_table: pd.DataFrame = submission.data_tables[table_name]
        for _, column_spec in self.metadata[table_name].columns.items():
            if column_spec.column_name not in data_table.columns:
                continue

            if column_spec.data_type not in self.NUMERIC_TYPES:
                continue

            series: pd.Series = data_table[column_spec.column_name]
            series = series[~series.isna()]
            ok_mask: pd.Series = series.apply(np.isreal)
            if not ok_mask.all():
                error_values = " ".join(list(set(series[~ok_mask])))[:200]
                self.errors.append(
                    f"ERROR Column {table_name}.{column_spec.column_name} has non-numeric values: {error_values}"
                )


@SpecificationRegistry.register()
class HasPrimaryKeySpecification(SpecificationBase):
    def is_satisfied_by(self, submission: SubmissionData, table_name: str) -> None:
        data_table: pd.DataFrame = submission.data_tables[table_name]
        if self.metadata[table_name].pk_name not in data_table.columns:
            self.errors.append(
                f'ERROR PK {table_name}.{self.metadata[table_name].pk_name} (table metadata) not found in data columns.'
            )

        if not any(c.is_pk for c in self.metadata[table_name].columns.values()):
            self.errors.append("ERROR Table {table_name} has no column with PK constraint")


@SpecificationRegistry.register()
class HasSystemIdSpecification(SpecificationBase):
    def is_satisfied_by(self, submission: SubmissionData, table_name: str) -> None:
        # Must have a system identity
        # if not submission.has_system_id(table_name):
        data_table: pd.DataFrame = submission.data_tables[table_name]

        if "system_id" not in data_table.columns:
            self.errors.append(f"ERROR {table_name} has no system id data column")
            return

        if data_table.system_id.isnull().values.any():
            self.errors.append(f"ERROR {table_name} has missing system id values")

        try:
            # duplicate_mask = data_table[~data_table.system_id.isna()].duplicated('system_id')
            duplicate_mask: pd.Series = data_table.duplicated("system_id")
            duplicates: list[int] = [int(x) for x in set(data_table[duplicate_mask].system_id)]
            if len(duplicates) > 0:
                error_values: str = " ".join([str(x) for x in duplicates])[:200]
                self.errors.append(f"ERROR Table {table_name} has DUPLICATE system ids: {error_values}")
        except Exception as _:
            self.warnings.append(f"WARNING! Duplicate check of {table_name}.system_id failed")


@SpecificationRegistry.register()
class IdColumnHasConstraintSpecification(SpecificationBase):
    def is_satisfied_by(self, submission: SubmissionData, table_name: str) -> None:
        for _, column_spec in self.metadata[table_name].columns.items():
            if column_spec.column_name[-3:] == "_id" and not (column_spec.is_fk or column_spec.is_pk):
                self.warnings.append(
                    f'WARNING! Column {table_name}.{column_spec.column_name}: ends with "_id" but NOT marked as PK/FK'
                )


@SpecificationRegistry.register()
class ForeignKeyColumnsHasValuesSpecification(SpecificationBase):
    def is_satisfied_by(self, submission: SubmissionData, table_name: str) -> None:
        """All submission tables MUST have a non null "system_id" """
        data_table: pd.DataFrame = submission.data_tables[table_name]
        for _, column_spec in self.metadata[table_name].columns.items():
            if len(data_table[column_spec.column_name]) == 0:
                continue
            if column_spec.is_fk:
                has_nan: bool = data_table[column_spec.column_name].isnull().values.any()
                all_nan: bool = data_table[column_spec.column_name].isnull().values.all()
                if all_nan and not column_spec.is_nullable:
                    self.errors.append(f"ERROR Foreign key column {table_name}.{column_spec.column_name} has no values")
                if has_nan and not column_spec.is_nullable:
                    self.errors.append(
                        f"ERROR Non-nullable foreign key column {table_name}.{column_spec.column_name} has missing values"
                    )


@SpecificationRegistry.register()
class ForeignKeyExistsAsPrimaryKeySpecification(SpecificationBase):
    def is_satisfied_by(self, submission: SubmissionData, table_name: str) -> None:
        """All submission tables MUST have a non null "system_id" """
        data_table: pd.DataFrame = submission.data_tables[table_name]
        for _, column_spec in self.metadata[table_name].columns.items():
            if len(data_table[column_spec.column_name]) == 0:
                continue
            if not column_spec.is_fk:
                continue
            fk_table_name: str = column_spec.fk_table_name
            if fk_table_name not in submission.data_tables:
                self.errors.append(f"ERROR Foreign key column {fk_table_name} missing in data")
                continue
            fk_table_spec: TableSpec = self.metadata[fk_table_name]
            if fk_table_spec.is_lookup_table:
                continue
            # fk_table: pd.DataFrame = submission.data_tables[fk_table_name]
            # if fk_table is None:
            #     self.warnings.append(f"ERROR Table {fk_table_name} referenced as FK in data by {table_name} but not found in submission.")
            #     continue
            # if not fk_system_id.isin(fk_table.system_id).all():
            #     self.warnings.append(
            #         f"ERROR FK value {table_name}.{column_spec.column_name} has values not found as PK in {fk_table_name}"
            #     )


@SpecificationRegistry.register()
class NoMissingColumnSpecification(SpecificationBase):
    def is_satisfied_by(self, submission: SubmissionData, table_name: str) -> None:
        """All fields in metadata.Table.Fields MUST exist in DataTable.columns"""
        meta_column_names: list[str] = sorted(self.metadata[table_name].columns.keys())
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
            self.errors.append(f"ERROR {table_name} has MISSING DATA columns: {', '.join(missing_column_names)}")

        if len(extra_column_names) > 0:
            self.warnings.append(f"WARNING {table_name} has EXTRA DATA columns: {', '.join(extra_column_names)}")


@SpecificationRegistry.register()
class LookupDataSpecification(SpecificationBase):
    def is_satisfied_by(self, submission: SubmissionData, table_name: str) -> None:
        if not submission.exists(table_name):
            return

        if not self.metadata[table_name].is_lookup_table:
            return

        data_table: pd.DataFrame = submission.data_tables[table_name]
        pk_name: str = self.metadata[table_name].pk_name

        if data_table[pk_name].isnull().any():
            self.errors.append(f"ERROR {table_name} new values not allowed for lookup table.")
