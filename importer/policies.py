from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np
import pandas as pd
from loguru import logger

from .configuration.inject import ConfigValue
from .metadata import Metadata, SeadSchema, Table
from .utility import Registry

if TYPE_CHECKING:
    from importer.submission import Submission


class SubmissionUpdateRegistry(Registry):
    items: dict[str, PolicyBase] = {}

    def get_sorted_items(self) -> list[PolicyBase]:
        return sorted(self.items.values(), key=lambda x: x.SORT_ORDER)


UpdatePolicies: SubmissionUpdateRegistry = SubmissionUpdateRegistry()


class PolicyBase:

    ID: str = "policy_base"
    SORT_ORDER: int = 0

    def __init__(self, metadata: Metadata, submission: Submission) -> None:
        self.metadata: Metadata = metadata
        self.submission: Submission = submission


@UpdatePolicies.register()
class AddPrimaryKeyColumnIfMissingPolicy(PolicyBase):
    """Adds a primary key column to the DataFrame if it is missing"""

    def apply(self) -> None:

        for table_name, data in self.submission.data_tables.items():
            table: Table = self.metadata[table_name]
            if table.pk_name not in data.columns:
                logger.info(
                    f"Added missing primary key column '{table.pk_name}' to '{table_name}' (assuming all new records)"
                )
                data[table.pk_name] = None


@UpdatePolicies.register()
class AddDefaultForeignKeyPolicy(PolicyBase):
    """Adds default FK value to DataFrame if it is missing"""

    ID: str = "add_default_fk_id_if_missing"

    def apply(self) -> pd.DataFrame:

        for table_name, cfg in (ConfigValue(f"policies.{self.ID}").resolve() or {}).items():

            if table_name not in self.submission:
                return

            if 'fk_name' not in cfg or 'fk_value' not in cfg:
                raise ValueError(
                    f"Table '{table_name}': 'fk_name' and 'fk_value' must be provided in config for policy '{self.ID}'"
                )

            fk_name: str = cfg['fk_name']
            fk_value: int = cfg['fk_value']

            data: pd.DataFrame = self.submission[table_name]

            if fk_name in data.columns:
                if data[fk_name].isnull().all():
                    logger.info(f"Added default value '{fk_value}' to '{fk_name}' in '{table_name}'")
                    data[fk_name] = fk_value
            else:
                logger.info(f"Added missing column '{fk_name}' to {table_name} using value '{fk_value}'")
                data[fk_name] = fk_value


@UpdatePolicies.register()
class IfLookupTableIsMissing_AddTableUsingSystemIdAsPublicId(PolicyBase):
    """Rule: if an FK table is missing then add the table using system_id as public_id.

    For table that is referenced by a foreign key,
        if the table is missing,
            add the table to the submission and assume that the table's system_id is the same as it's public primary key

    Assumptions:
        The table must be a lookup table
        The foreign keys used in the submission MUST be equal to the public primary key of the table
        The rule is applied to the table only if it exists in configuration (YAML) file
    """

    ID: str = "if_lookup_table_is_missing_add_table_using_system_id_as_public_id"

    def apply(self) -> None:

        for table_name in ConfigValue(f"policies.{self.ID}").resolve() or {}:

            if table_name in self.submission:
                continue

            referenced_keys: list[str] = sorted(self.submission.get_referenced_keyset(self.metadata, table_name))

            meta_table: Table = self.metadata[table_name]
            pk_name: str = meta_table.pk_name

            self.submission.data_tables[table_name] = pd.DataFrame(
                {'system_id': referenced_keys, pk_name: list(referenced_keys)}
            )

            logger.info(f"Added table '{table_name}' added to submission with identity system_id/{pk_name} mapping")


@UpdatePolicies.register()
class UpdateTypesBasedOnSeadSchema(PolicyBase):
    """Rule: update data types based on SEAD schema

    For each table in the submission,
        update the data types of the columns based on the SEAD schema
    """

    ID: str = "update_types_based_on_sead_schema"

    def apply(self) -> None:

        for table_name in self.submission.data_tables:

            data_table: pd.DataFrame = self.submission.data_tables[table_name]
            table_spec: Table = self.metadata[table_name]

            for column_name, column_spec in table_spec.columns.items():

                if column_name not in data_table.columns:
                    continue

                if column_spec.data_type == 'smallint':
                    data_table[column_name] = data_table[column_name].astype('Int16')
                elif column_spec.data_type == 'integer':
                    data_table[column_name] = data_table[column_name].astype('Int32')
                elif column_spec.data_type == 'bigint':
                    data_table[column_name] = data_table[column_name].astype('Int64')


@UpdatePolicies.register()
class SetPublicIdToNegativeSystemIdForNewLookups(PolicyBase):
    """Rule: assign temporary public primary key to new lookup table rows.

    For new lookup table rows,
        set the public primary key to the negative of the system_id
            if all public primary keys are missing
    In this case, the public primary key is assigned upon submission commit to the database
    """

    ID: str = "set_public_id_to_negative_system_id_for_new_lookups"

    def apply(self) -> None:

        for table_name in self.submission.data_tables:

            if not self.metadata[table_name].is_lookup:
                continue

            data_table: pd.DataFrame = self.submission.data_tables[table_name]

            pk_name: str = self.metadata[table_name].pk_name

            if pk_name not in data_table.columns:
                continue

            if data_table[pk_name].isnull().any():
                data_table.loc[data_table[pk_name].isnull(), pk_name] = -data_table['system_id']
                data_table[pk_name] = data_table[pk_name].astype(int)


@UpdatePolicies.register()
class IfSystemIdIsMissing_SetSystemIdToPublicId(PolicyBase):
    """Rule: assign temporary public primary key to new lookup table rows.

    For new lookup table rows,
        set the public primary key to the negative of the system_id
            if all public primary keys are missing
    In this case, the public primary key is assigned upon submission commit to the database
    """

    ID: str = "if_system_id_is_missing_set_system_id_to_public_id"

    def apply(self) -> None:
        """For each table in index, update system_id to public_id if isnan. This should be avoided though."""
        for table_name in self.submission.data_tables:

            data_table: pd.DataFrame = self.submission.data_tables[table_name]
            table_spec: Table = self.metadata[table_name]

            pk_name: str = table_spec.pk_name

            if pk_name == "ceramics_id":
                pk_name = "ceramic_id"

            if data_table is None or pk_name not in data_table.columns:
                continue

            if "system_id" not in data_table.columns:
                raise ValueError(f'critical error Table {table_name} has no column named "system_id"')

            # Update system_id to public_id if isnan. This should be avoided though.
            data_table.loc[np.isnan(data_table.system_id), "system_id"] = data_table.loc[
                np.isnan(data_table.system_id), pk_name
            ]


@UpdatePolicies.register()
class IfForeignKeyValueIsMissing_AddIdentityMappingToForeignKeyTable(PolicyBase):
    """Any foreign key value that is missing in the submission is added to the foreign key table."""

    ID: str = "if_foreignkey_value_is_missing_add_identity_mapping_to_foreignkey_table"
    SORT_ORDER: int = 1

    def apply(self) -> pd.DataFrame:

        sead_schema: SeadSchema = self.metadata.sead_schema

        for table in sead_schema.lookup_tables:

            table_name: str = table.table_name

            referenced_keys: list[int] = sorted(self.submission.get_referenced_keyset(self.metadata, table_name))

            if not referenced_keys:
                continue

            if table_name not in self.submission:
                """This case is handled by another policy"""
                continue

            data_table: pd.DataFrame = self.submission.data_tables[table_name]
            pk_name: str = sead_schema[table_name].pk_name

            missing_keys: list[int] = [k for k in referenced_keys if k not in data_table['system_id'].values]

            if not missing_keys:
                continue

            template: dict[str, int] = {c: None for c in data_table.columns}
            rows_to_add: list[dict[str, int]] = [
                template | {'system_id': system_id, pk_name: system_id} for system_id in missing_keys
            ]
            data_table = pd.concat([data_table, pd.DataFrame(rows_to_add)], ignore_index=True)

            self.submission.data_tables[table_name] = data_table

            