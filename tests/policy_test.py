from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd

from importer.metadata import Metadata, SeadSchema, Table
from importer.policies import (
    AddDefaultForeignKeyPolicy,
    AddPrimaryKeyColumnIfMissingPolicy,
    IfForeignKeyValueIsMissingAddIdentityMappingToForeignKeyTable,
    IfLookupTableIsMissingAddTableUsingSystemIdAsPublicId,
    IfSystemIdIsMissingSetSystemIdToPublicId,
    PolicyBase,
    UpdateTypesBasedOnSeadSchema,
)
from importer.submission import Submission


def test_initialization():
    metadata = MagicMock(spec=Metadata)
    submission = MagicMock(spec=Submission)

    policy = PolicyBase(metadata=metadata, submission=submission)

    assert policy.metadata == metadata
    assert policy.submission == submission


def test_get_policy_id():
    metadata = MagicMock(spec=Metadata)
    submission = MagicMock(spec=Submission)

    policy = PolicyBase(metadata=metadata, submission=submission)
    assert policy.get_policy_id() == "policy_base"


def test_initialization():
    metadata = MagicMock(spec=Metadata)
    submission = MagicMock(spec=Submission)

    policy = PolicyBase(metadata=metadata, submission=submission)

    assert policy.metadata == metadata
    assert policy.submission == submission


def test_get_policy_id():
    metadata = MagicMock(spec=Metadata)
    submission = MagicMock(spec=Submission)

    policy = PolicyBase(metadata=metadata, submission=submission)
    assert policy.get_policy_id() == "policy_base"


def test_add_primary_key_column_if_missing_policy():
    metadata = MagicMock(spec=Metadata)
    submission = MagicMock(spec=Submission)
    table = MagicMock(spec=Table)
    table.pk_name = "id"
    metadata.__getitem__.return_value = table
    submission.data_tables = {"table1": pd.DataFrame(columns=["col1", "col2"])}

    policy = AddPrimaryKeyColumnIfMissingPolicy(metadata=metadata, submission=submission)
    policy.apply()

    assert "id" in submission.data_tables["table1"].columns


def test_add_default_foreign_key_policy():
    metadata = MagicMock(spec=Metadata)
    submission = MagicMock(spec=Submission)
    submission.data_tables = {"table1": pd.DataFrame({"fk_col": [None, None]})}
    config_value = MagicMock()
    config_value.resolve.return_value = {"table1": {"fk_name": "fk_col", "fk_value": 1}}
    submission.__contains__.side_effect = lambda x: x in submission.data_tables

    with patch("importer.policies.ConfigValue", return_value=config_value):
        policy = AddDefaultForeignKeyPolicy(metadata=metadata, submission=submission)
        policy.apply()

    assert (submission.data_tables["table1"]["fk_col"] == 1).all()


def test_if_lookup_table_is_missing_add_table_using_system_id_as_public_id():
    metadata = MagicMock(spec=Metadata)
    submission = MagicMock(spec=Submission)
    table = MagicMock(spec=Table)
    table.pk_name = "id"
    metadata.__getitem__.return_value = table
    submission.get_referenced_keyset.return_value = [1, 2, 3]
    submission.data_tables = {}

    config_value = MagicMock()
    config_value.resolve.return_value = {"table1"}

    with patch("importer.policies.ConfigValue", return_value=config_value):
        policy = IfLookupTableIsMissingAddTableUsingSystemIdAsPublicId(metadata=metadata, submission=submission)
        policy.apply()

    assert "table1" in submission.data_tables
    assert list(submission.data_tables["table1"]["system_id"]) == [1, 2, 3]
    assert list(submission.data_tables["table1"]["id"]) == [1, 2, 3]


def test_update_types_based_on_sead_schema():
    metadata = MagicMock(spec=Metadata)
    submission = MagicMock(spec=Submission)
    table = MagicMock(spec=Table)
    table.columns = {
        "col1": MagicMock(data_type="smallint"),
        "col2": MagicMock(data_type="integer"),
        "col3": MagicMock(data_type="bigint"),
    }
    metadata.__getitem__.return_value = table
    submission.data_tables = {
        "table1": pd.DataFrame(
            {
                "col1": [1, 2, 3],
                "col2": [4, 5, 6],
                "col3": [7, 8, 9],
            }
        )
    }

    policy = UpdateTypesBasedOnSeadSchema(metadata=metadata, submission=submission)
    policy.apply()

    assert submission.data_tables["table1"]["col1"].dtype == "Int16"
    assert submission.data_tables["table1"]["col2"].dtype == "Int32"
    assert submission.data_tables["table1"]["col3"].dtype == "Int64"


def test_if_system_id_is_missing_set_system_id_to_public_id():
    metadata = MagicMock(spec=Metadata)
    submission = MagicMock(spec=Submission)
    table = MagicMock(spec=Table)
    table.pk_name = "id"
    metadata.__getitem__.return_value = table
    submission.data_tables = {
        "table1": pd.DataFrame(
            {
                "id": [1, 2, 3],
                "system_id": [np.nan, np.nan, np.nan],
            }
        )
    }

    policy = IfSystemIdIsMissingSetSystemIdToPublicId(metadata=metadata, submission=submission)
    policy.apply()

    assert list(submission.data_tables["table1"]["system_id"]) == [1, 2, 3]


def test_if_foreign_key_value_is_missing_add_identity_mapping_to_foreign_key_table():
    metadata = MagicMock(spec=Metadata)
    submission = MagicMock(spec=Submission)
    sead_schema = MagicMock(spec=SeadSchema)
    table = MagicMock(spec=Table)
    table.pk_name = "public_id"
    table.table_name = "tbl_table"
    sead_schema.lookup_tables = [table]
    metadata.sead_schema = sead_schema
    sead_schema.__getitem__.side_effect = lambda x: table
    submission.get_referenced_keyset.return_value = [1, 2, 3]
    submission.data_tables = {table.table_name: pd.DataFrame({"system_id": [1], table.pk_name: [1]})}
    submission.__contains__.side_effect = lambda x: x in submission.data_tables

    policy = IfForeignKeyValueIsMissingAddIdentityMappingToForeignKeyTable(metadata=metadata, submission=submission)
    policy.apply()

    assert list(submission.data_tables[table.table_name]["system_id"]) == [1, 2, 3]
    assert list(submission.data_tables[table.table_name][table.pk_name]) == [1, 2, 3]
