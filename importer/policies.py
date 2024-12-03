from __future__ import annotations

from loguru import logger
import pandas as pd
from .configuration.inject import ConfigValue
from .metadata import Table, Metadata
from .utility import Registry
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from importer.submission import Submission


class SubmissionUpdateRegistry(Registry):
    items: dict = {}


UpdatePolicies: SubmissionUpdateRegistry = SubmissionUpdateRegistry()


class PolicyBase:
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
                    f"INFO: adding missing primary key column '{table.pk_name}' to '{table_name}' (assuming all new records)"
                )
                data[table.pk_name] = None


@UpdatePolicies.register()
class AddDefaultContactTypeIfMissingPolicy(PolicyBase):
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
                    logger.info(f"INFO: adding default value '{fk_value}' to '{fk_name}' in '{table_name}'")
                    data[fk_name] = fk_value
            else:
                logger.info(f"INFO: adding missing column '{fk_name}' to {table_name} using value '{fk_value}'")
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

            logger.info(f"INFO: table '{table_name}' added to submission with system_id")


@UpdatePolicies.register()
class SetPublicIdToNegativeSystemIdForNewLookups(PolicyBase):
    """ Rule: assign temporary public primary key to new lookup table rows.

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
