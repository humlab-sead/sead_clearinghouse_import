import abc
import logging
from dataclasses import dataclass, field
from fnmatch import fnmatch

import numpy as np
import pandas as pd
from loguru import logger

from importer.configuration.inject import ConfigValue

from .metadata import Metadata, Table
from .submission import Submission
from .utility import Registry, log_decorator


class SpecificationRegistry(Registry):
    items: dict = {}


@dataclass
class SpecificationMessages:
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    infos: list[str] = field(default_factory=list)

    def uniqify(self) -> None:
        self.errors = sorted(set(self.errors))
        self.warnings = sorted(set(self.warnings))
        self.infos = sorted(set(self.infos))


class SpecificationError(Exception):
    def __init__(self, messages: SpecificationMessages) -> None:
        super().__init__("Submission specification failed")
        self.messages: SpecificationMessages = messages


class SpecificationBase(abc.ABC):
    def __init__(self, metadata: Metadata, messages: SpecificationMessages, ignore_columns: list[str]) -> None:
        self.metadata: Metadata = metadata
        self.messages: SpecificationMessages = messages
        self.ignore_columns: list[str] = ignore_columns or ConfigValue("options:ignore_columns").resolve() or []

    def is_ignored(self, column_name: str) -> bool:
        return any(fnmatch(column_name, x) for x in self.ignore_columns)

    @property
    def errors(self) -> list[str]:
        return self.messages.errors

    @property
    def warnings(self) -> list[str]:
        return self.messages.warnings

    @property
    def infos(self) -> list[str]:
        return self.messages.infos

    def clear(self) -> None:
        self.messages.errors = []
        self.messages.warnings = []
        self.messages.infos = []

    def warn(self, message: str) -> None:
        self.warnings.append(f'{message}')

    def error(self, message: str) -> None:
        self.errors.append(f'{message}')

    def info(self, message: str) -> None:
        self.infos.append(f'{message}')

    def get_columns(self, table_name: str) -> list[Table]:
        return [column for column in self.metadata[table_name].columns.values() if self.is_ignored(column.column_name)]

    @abc.abstractmethod
    def is_satisfied_by(self, submission: Submission, table_name: str) -> None: ...


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
    def is_satisfied_by(self, submission: Submission, _: str = None) -> bool:
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
            for table_name in submission.data_tables.keys():
                specification.is_satisfied_by(submission, table_name)

        self.messages.uniqify()

        self.log_messages()

        if self.raise_errors and len(specification.errors) > 0:
            raise SpecificationError(self.messages)

        return len(self.errors) == 0

    def log_messages(self) -> None:
        for message in self.errors:
            logger.error(message)
        for message in self.warnings:
            logger.warning(message)
        for message in self.infos:
            logger.info(message)


@SpecificationRegistry.register()
class SubmissionTableExistsSpecification(SpecificationBase):
    """Specification class that tests if table exists in submission"""

    def is_satisfied_by(self, submission: Submission, table_name: str) -> None:
        if table_name not in submission:
            self.error(f"Table '{table_name}' not defined as submission table")


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
        ("bigint", "int64"): True,
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

    def is_satisfied_by(self, submission: Submission, table_name: str) -> None:
        data_table: pd.DataFrame = submission.data_tables[table_name]
        if len(data_table) == 0:
            """Cannot determine type if table is empty"""
            return

        for column in self.get_columns(table_name):
            if column.column_name not in data_table.columns:
                continue
            data_column_type: str = data_table.dtypes[column.column_name].name
            if all(data_table[column.column_name].isna()):
                continue
            if not self.TYPE_COMPATIBILITY_MATRIX.get((column.data_type, data_column_type), False):
                self.warn(f"type clash: {table_name}.{column.column_name} {column.data_type}<=>{data_column_type}")


@SpecificationRegistry.register()
class SubmissionTableTypesSpecification(SpecificationBase):
    NUMERIC_TYPES: list[str] = ["numeric", "integer", "smallint"]

    def is_satisfied_by(self, submission: Submission, table_name: str) -> None:
        data_table: pd.DataFrame = submission.data_tables[table_name]
        for column in self.get_columns(table_name):
            if column.column_name not in data_table.columns:
                continue

            if column.data_type not in self.NUMERIC_TYPES:
                continue

            series: pd.Series = data_table[column.column_name]
            series = series[~series.isna()]
            ok_mask: pd.Series = series.apply(np.isreal)
            if not ok_mask.all():
                error_values = " ".join(list(set(series[~ok_mask])))[:200]
                self.error(f"Column '{table_name}.{column.column_name}' has non-numeric values: '{error_values}'")


@SpecificationRegistry.register()
class HasPrimaryKeySpecification(SpecificationBase):
    def is_satisfied_by(self, submission: Submission, table_name: str) -> None:
        data_table: pd.DataFrame = submission.data_tables[table_name]
        if self.metadata[table_name].pk_name not in data_table.columns:
            self.error(
                f"Primary key column '{table_name}.{self.metadata[table_name].pk_name}' (table metadata) not in data columns."
            )

        if not any(c.is_pk for c in self.metadata[table_name].columns.values()):
            self.error(f"Table '{table_name}' has no column with PK constraint")


@SpecificationRegistry.register()
class HasSystemIdSpecification(SpecificationBase):
    def is_satisfied_by(self, submission: Submission, table_name: str) -> None:
        # Must have a system identity
        # if not submission.has_system_id(table_name):
        data_table: pd.DataFrame = submission.data_tables[table_name]

        if "system_id" not in data_table.columns:
            self.error(f"Table {table_name} has no system id data column")
            return

        if data_table.system_id.isnull().values.any():
            self.error(f"Table {table_name} has missing system id values")

        try:
            # duplicate_mask = data_table[~data_table.system_id.isna()].duplicated('system_id')
            duplicate_mask: pd.Series = data_table.duplicated("system_id")
            duplicates: list[int] = [int(x) for x in set(data_table[duplicate_mask].system_id)]
            if len(duplicates) > 0:
                error_values: str = " ".join([str(x) for x in duplicates])[:200]
                self.error(f"Table {table_name} has DUPLICATE system ids: {error_values}")
        except Exception as _:
            self.warn(f"Duplicate check of {table_name}.system_id failed")


@SpecificationRegistry.register()
class IdColumnHasConstraintSpecification(SpecificationBase):
    def is_satisfied_by(self, submission: Submission, table_name: str) -> None:
        for column in self.get_columns(table_name):
            if column.column_name[-3:] == "_id" and not (column.is_fk or column.is_pk):
                self.warn(f'Column {table_name}.{column.column_name}: ends with "_id" but NOT marked as PK/FK')


@SpecificationRegistry.register()
class ForeignKeyColumnsHasValuesSpecification(SpecificationBase):
    def is_satisfied_by(self, submission: Submission, table_name: str) -> None:
        """Foreign key columns must have values"""
        data_table: pd.DataFrame = submission.data_tables[table_name]

        if len(data_table) == 0:
            return

        if submission.is_lookup(table_name):
            if not submission.has_new_rows(table_name):
                return

        for column in self.get_columns(table_name):

            if not column.is_fk:
                continue

            if column.column_name not in data_table.columns:
                if not column.is_nullable:
                    self.error(f"Foreign key column '{table_name}.{column.column_name}' not in data")
                    continue
                else:
                    self.warn(f"Foreign key column '{table_name}.{column.column_name}' not in data (but is nullable)")
                continue

            has_nan: bool = data_table[column.column_name].isnull().values.any()
            all_nan: bool = data_table[column.column_name].isnull().values.all()

            if all_nan and not column.is_nullable:
                self.error(f"Foreign key column '{table_name}.{column.column_name}' has no values")

            if has_nan and not column.is_nullable:
                self.error(f"Non-nullable foreign key column '{table_name}.{column.column_name}' has missing values")


@SpecificationRegistry.register()
class ForeignKeyExistsAsPrimaryKeySpecification(SpecificationBase):
    def is_satisfied_by(self, submission: Submission, table_name: str) -> None:
        """All submission tables MUST have a non null "system_id" """
        data_table: pd.DataFrame = submission.data_tables[table_name]
        if len(data_table) == 0:
            return

        if submission.is_lookup(table_name):
            if not submission.has_new_rows(table_name):
                return

        for column in self.get_columns(table_name):

            if not column.is_fk:
                continue

            if column.column_name not in data_table.columns:
                if column.is_nullable:
                    self.warn(f"Foreign key column '{table_name}.{column.column_name}' not in data (but is nullable)")
                else:
                    self.error(f"Foreign key column '{table_name}.{column.column_name}' not in data")
                continue

            fk_has_data: bool = not data_table[column.column_name].isnull().all()

            fk_table_name: str = column.fk_table_name
            if fk_table_name not in submission.data_tables:
                msg: str = f"Foreign key table '{fk_table_name}' referenced by '{table_name}'"
                if column.is_nullable and not fk_has_data:
                    self.warn(f"{msg} missing in data (but is nullable)")
                elif column.is_nullable:
                    self.error(f"{msg} FK has values but target table not found in submission")
                else:
                    self.error(f"{msg} missing in data and NOT nullable")
                continue
            fk_table: Table = self.metadata[fk_table_name]
            if fk_table.is_lookup:
                continue
            # fk_table: pd.DataFrame = submission.data_tables[fk_table_name]
            # if fk_table is None:
            #     self.warn(f"ERROR Table {fk_table_name} referenced as FK in data by {table_name} but not found in submission.")
            #     continue
            # if not fk_system_id.isin(fk_table.system_id).all():
            #     self.warn(
            #         f"ERROR FK value {table_name}.{column_spec.column_name} has values not found as PK in {fk_table_name}"
            #     )


@SpecificationRegistry.register()
class NoMissingColumnSpecification(SpecificationBase):
    def is_satisfied_by(self, submission: Submission, table_name: str) -> None:
        """All fields in metadata.Table.Fields MUST exist in DataTable.columns"""

        data_table: pd.DataFrame = submission.data_tables[table_name] if table_name in submission else None
        meta_table: Table = self.metadata[table_name]

        data_column_names: list[str] = (
            sorted(data_table.columns.values.tolist()) if data_table is not None and table_name in self.metadata else []
        )

        if set(data_column_names) == {'system_id', meta_table.pk_name}:
            """This is a lookup table with only system_id and public_id"""
            return

        missing_column_names: set[str] = set(
            x for x in meta_table.column_names(skip_nullable=True) if not self.is_ignored(x)
        ) - set(data_column_names)

        if len(missing_column_names) > 0:
            self.error(
                f"Table {table_name} has MISSING NON-NULLABLE data columns: {', '.join(sorted(missing_column_names))}"
            )

        missing_nullable_column_names: set[str] = set(
            x for x in meta_table.nullable_column_names() if not self.is_ignored(x)
        ) - set(data_column_names)

        if len(missing_nullable_column_names) > 0:
            self.warn(
                f"Table {table_name} has MISSING NULLABLE data columns: {', '.join(sorted(missing_nullable_column_names))}"
            )

        extra_column_names = list(
            set(x for x in data_column_names if not self.is_ignored(x))
            - set(meta_table.column_names(skip_nullable=False))
            - set(["system_id"])
        )

        if len(extra_column_names) > 0:
            self.warn(f"Table {table_name} has EXTRA data columns: {', '.join(extra_column_names)}")


@SpecificationRegistry.register()
class NonNullableColumnHasValueSpecification(SpecificationBase):
    def is_satisfied_by(self, submission: Submission, table_name: str) -> None:
        """All fields in metadata.Table.Fields MUST exist in DataTable.columns"""

        if table_name not in submission or table_name not in self.metadata:
            return

        data: pd.DataFrame = submission.data_tables[table_name]
        table: Table = self.metadata[table_name]

        non_nullable_columns: list[str] = {
            x
            for x in data.columns
            if x in table.column_names(skip_nullable=True)
            and not self.is_ignored(x)
            and not table.columns[x].is_pk
            and not table.columns[x].is_fk
            and x != "system_id"
        }

        for column_name in non_nullable_columns:
            if data[column_name].isnull().any():
                self.error(f"Table {table_name} has NULL values in non-nullable column {column_name}")


# DISABLED: @SpecificationRegistry.register()
class NewLookupDataIsNotAllowedSpecification(SpecificationBase):
    DISABLED: bool = True

    def is_satisfied_by(self, submission: Submission, table_name: str) -> None:
        if self.DISABLED:
            logger.warning("NewLookupDataIsNotAllowedSpecification is disabled")
            return

        if table_name not in submission:
            return

        if not self.metadata[table_name].is_lookup:
            return

        data_table: pd.DataFrame = submission.data_tables[table_name]

        pk_name: str = self.metadata[table_name].pk_name

        if pk_name not in data_table.columns:
            self.error(f"Table {table_name} is missing primary key column {pk_name}")
            return

        if data_table[pk_name].isnull().any():
            self.error(f"Table {table_name}, new values not allowed for lookup table.")
