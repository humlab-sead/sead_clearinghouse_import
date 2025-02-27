import os
import xml.etree.ElementTree as ET
from typing import Any, Iterator

import pandas as pd
import pytest

from importer.configuration.config import Config
from importer.metadata import Metadata
from importer.process import ImportService, Options
from importer.specification import (
    ForeignKeyColumnsHasValuesSpecification,
    SpecificationMessages,
    SubmissionSpecification,
)
from importer.submission import Submission
from importer.utility import create_db_uri
from importer import policies


@pytest.fixture(scope="module")
def metadata(cfg: Config) -> Metadata:
    db_opts: dict[str, Any] = cfg.get("options:database") | cfg.get("test:dendrochronology:database")
    uri: str = create_db_uri(**db_opts)
    return Metadata(uri)


@pytest.fixture(scope="module")
def submission(cfg: Config, metadata: Metadata) -> Iterator[Submission]:
    source: str = cfg.get("test:dendrochronology:living_tree:source:filename")
    return Submission.load(metadata=metadata, source=source, apply_policies=True)

def test_pk_set(metadata: Metadata):
    keys: set[int] = metadata.get_primary_keys("tbl_sites")

    assert keys

def test_load_living_tree(submission: Submission):
    assert submission is not None
    assert submission.metadata is not None
    assert submission.data_tables is not None
    assert len(submission.data_tables) > 0


def test_living_tree_tables_specifications(submission: Submission, cfg: Config):
    specification: SubmissionSpecification = SubmissionSpecification(
        metadata=submission.metadata, ignore_columns=cfg.get("options:ignore_columns"), raise_errors=False
    )
    specification.is_satisfied_by(submission)
    assert specification.messages.errors == []


def test_living_tree_tables_specifications_bugg(submission: Submission, cfg: Config):
    specification: ForeignKeyColumnsHasValuesSpecification = ForeignKeyColumnsHasValuesSpecification(
        metadata=submission.metadata,
        messages=SpecificationMessages(),
        ignore_columns=cfg.get("options:ignore_columns"),
    )
    specification.is_satisfied_by(submission, 'tbl_dataset_submissions')
    assert specification.messages.errors == []
    assert specification.messages.warnings == []


def test_to_lookups_sql(submission: Submission):

    filename: str = 'tests/output/lookups.sql'

    os.makedirs(os.path.dirname(filename), exist_ok=True)
    if os.path.isfile(filename):
        os.unlink(filename)

    submission.to_lookups_sql(filename)

    assert os.path.isfile(filename)


def test_loaded_living_tree_source(submission: Submission, cfg: Config):
    source: str = cfg.get("test:dendrochronology:submission:source:filename")

    assert submission.data_tables is not None

    empty_tables: list[str] = [n for n, df in submission.data_tables.items() if len(df) == 0]

    assert len(empty_tables) == 0, f"Empty tables found: {empty_tables}"

    # Verify that no table in the submission is keyed by excel sheet name for aliased tables
    assert all(n.excel_sheet not in submission.data_tables for n in submission.metadata.sead_schema.aliased_tables)

    with pd.ExcelFile(source) as reader:
        # Verify that all excel sheet names are in the submission data tables
        excel_sheet_names: set[str] = set(reader.sheet_names)
        excel_table_names: set[str] = {
            n for n, t in submission.metadata.sead_schema.items() if t.excel_sheet in excel_sheet_names
        }

        assert all(table_name in submission.data_tables for table_name in excel_table_names)


def test_import_living_tree_submission(submission: Submission, cfg: Config):

    opts: Options = Options(
        **{
            'filename': cfg.get("test:dendrochronology:submission:source:filename"),
            'data_types': 'submission',
            'database': cfg.get("options:database"),
            'output_folder': 'tests/output',
            'skip': False,
            'submission_id': None,
            'table_names': None,
            'xml_filename': None,
            'check_only': False,
            'register': True,
            'transfer_format': 'csv',
        }
    )

    if os.path.isfile(opts.target):
        os.remove(opts.target)

    service: ImportService = ImportService(metadata=submission.metadata, opts=opts)

    service.process(submission=submission)

    assert not service.specification.messages.errors

    assert os.path.isfile(opts.target)

    with open(opts.target, "r") as f:
        root: ET.Element = ET.fromstring(f.read())

    exported_java_classes: set[str] = {child.tag for child in root}

    assert 'TblContacts' in exported_java_classes

    expected_java_classes: set[str] = {submission.metadata[t].java_class for t in submission.data_table_names}

    assert all(t in exported_java_classes for t in expected_java_classes)


# Policy tests in living tree data


@pytest.fixture(scope="module")
def unprocessed_submission(cfg: Config, metadata: Metadata) -> Iterator[Submission]:
    return Submission.load(
        metadata=metadata, source=cfg.get("test:dendrochronology:living_tree:source:filename"), apply_policies=False
    )


def test_add_primary_key_column_if_missing_policy(unprocessed_submission: Submission):
    policy: policies.AddPrimaryKeyColumnIfMissingPolicy = policies.AddPrimaryKeyColumnIfMissingPolicy(
        metadata=unprocessed_submission.metadata, submission=unprocessed_submission
    )
    policy.apply()
    assert not policy.logs


def test_add_default_foreign_key_policy(unprocessed_submission: Submission):
    policy: policies.AddDefaultForeignKeyPolicy = policies.AddDefaultForeignKeyPolicy(
        metadata=unprocessed_submission.metadata, submission=unprocessed_submission
    )
    policy.apply()
    assert not policy.logs


def test_if_table_is_missing_add_table_using_system_id_as_public_id(unprocessed_submission: Submission):
    policy: policies.AddIdentityMappingSystemIdToPublicIdPolicy = (
        policies.AddIdentityMappingSystemIdToPublicIdPolicy(
            metadata=unprocessed_submission.metadata, submission=unprocessed_submission
        )
    )
    policy.apply()
    assert len(policy.logs) > 0

def test_statistics(unprocessed_submission: Submission):

    statistics = []

    for table_name in unprocessed_submission.data_table_names:
        # data: pd.DataFrame = unprocessed_submission.data_tables[table_name]
        table: policies.Table = unprocessed_submission.metadata[table_name]

        for column in table.columns.values():

            if column.is_fk:
                fk_table_name: str = column.fk_table_name
                fk_column_name: str = column.fk_column_name
                fk_table_exists: bool = fk_table_name in unprocessed_submission.data_tables
                                
                statistics.append((fk_table_name, fk_column_name, fk_table_exists, table_name, column.column_name))

    df = pd.DataFrame(statistics, columns=['fk_table_name', 'fk_column_name', 'fk_table_exists', 'table_name', 'column_name'])
    df.to_csv('living_tree_statistics.csv', index=False)

    assert True


    # metadata = MagicMock(spec=Metadata)
    # submission = MagicMock(spec=Submission)
    # table = MagicMock(spec=Table)
    # table.columns = {
    #     "col1": MagicMock(data_type="smallint"),
    #     "col2": MagicMock(data_type="integer"),
    #     "col3": MagicMock(data_type="bigint"),
    # }
#     metadata.__getitem__.return_value = table
#     submission.data_tables = {
#         "table1": pd.DataFrame(
#             {
#                 "col1": [1, 2, 3],
#                 "col2": [4, 5, 6],
#                 "col3": [7, 8, 9],
#             }
#         )
#     }

#     policy = UpdateTypesBasedOnSeadSchema(metadata=metadata, submission=submission)
#     policy.apply()

#     assert submission.data_tables["table1"]["col1"].dtype == "Int16"
#     assert submission.data_tables["table1"]["col2"].dtype == "Int32"
#     assert submission.data_tables["table1"]["col3"].dtype == "Int64"


# def test_if_system_id_is_missing_set_system_id_to_public_id():
#     metadata = MagicMock(spec=Metadata)
#     submission = MagicMock(spec=Submission)
#     table = MagicMock(spec=Table)
#     table.pk_name = "id"
#     metadata.__getitem__.return_value = table
#     submission.data_tables = {
#         "table1": pd.DataFrame(
#             {
#                 "id": [1, 2, 3],
#                 "system_id": [np.nan, np.nan, np.nan],
#             }
#         )
#     }

#     policy = IfSystemIdIsMissingSetSystemIdToPublicId(metadata=metadata, submission=submission)
#     policy.apply()

#     assert list(submission.data_tables["table1"]["system_id"]) == [1, 2, 3]


# def test_if_foreign_key_value_is_missing_add_identity_mapping_to_foreign_key_table():
#     metadata = MagicMock(spec=Metadata)
#     submission = MagicMock(spec=Submission)
#     sead_schema = MagicMock(spec=SeadSchema)
#     table = MagicMock(spec=Table)
#     table.pk_name = "public_id"
#     table.table_name = "tbl_table"
#     sead_schema.lookup_tables = [table]
#     metadata.sead_schema = sead_schema
#     sead_schema.__getitem__.side_effect = lambda x: table
#     submission.get_referenced_keyset.return_value = [1, 2, 3]
#     submission.data_tables = {table.table_name: pd.DataFrame({"system_id": [1], table.pk_name: [1]})}
#     submission.__contains__.side_effect = lambda x: x in submission.data_tables

#     policy = IfForeignKeyValueIsMissingAddIdentityMappingToForeignKeyTable(metadata=metadata, submission=submission)
#     policy.apply()

#     assert list(submission.data_tables[table.table_name]["system_id"]) == [1, 2, 3]
#     assert list(submission.data_tables[table.table_name][table.pk_name]) == [1, 2, 3]


# def test_if_lookup_with_no_new_data_then_keep_only_system_id_public_id__not_lookup():
#     metadata = MagicMock(spec=Metadata)
#     submission = MagicMock(spec=Submission)
#     table = MagicMock(spec=Table)
#     table.is_lookup = False
#     metadata.__getitem__.return_value = table
#     submission.data_tables = {"table1": pd.DataFrame(columns=["system_id", "public_id", "col1", "col2"])}
#     policy: PolicyBase = IfLookupWithNoNewDataThenKeepOnlySystemIdPublicId(metadata=metadata, submission=submission)
#     policy.update()

#     assert "col1" in submission.data_tables["table1"].columns
#     assert "col2" in submission.data_tables["table1"].columns


# def test_if_lookup_with_no_new_data_then_keep_only_system_id_public_id__pk_not_in_data_table():
#     metadata = MagicMock(spec=Metadata)
#     submission = MagicMock(spec=Submission)
#     table = MagicMock(spec=Table)
#     table.is_lookup = True
#     table.pk_name = "public_id"
#     metadata.__getitem__.return_value = table
#     submission.data_tables = {"table1": pd.DataFrame(columns=["system_id", "col1", "col2"])}

#     policy: PolicyBase = IfLookupWithNoNewDataThenKeepOnlySystemIdPublicId(metadata=metadata, submission=submission)
#     policy.update()

#     assert "col1" in submission.data_tables["table1"].columns
#     assert "col2" in submission.data_tables["table1"].columns


# def test_if_lookup_with_no_new_data_then_keep_only_system_id_public_id__all_pk_values_null():
#     metadata = MagicMock(spec=Metadata)
#     submission = MagicMock(spec=Submission)
#     table = MagicMock(spec=Table)
#     table.is_lookup = True
#     table.pk_name = "public_id"
#     metadata.__getitem__.return_value = table
#     submission.data_tables = {
#         "table1": pd.DataFrame(
#             {"system_id": [1, 2, 3], "public_id": [None, None, None], "col1": [4, 5, 6], "col2": [7, 8, 9]}
#         )
#     }

#     policy: PolicyBase = IfLookupWithNoNewDataThenKeepOnlySystemIdPublicId(metadata=metadata, submission=submission)
#     policy.update()

#     assert "col1" in submission.data_tables["table1"].columns
#     assert "col2" in submission.data_tables["table1"].columns


# def test_not_all_pk_values_null():
#     metadata = MagicMock(spec=Metadata)
#     submission = MagicMock(spec=Submission)
#     table = MagicMock(spec=Table)
#     table.is_lookup = True
#     table.pk_name = "public_id"
#     metadata.__getitem__.return_value = table
#     submission.data_tables = {
#         "table1": pd.DataFrame(
#             {"system_id": [1, 2, 3], "public_id": [None, 2, None], "col1": [4, 5, 6], "col2": [7, 8, 9]}
#         )
#     }

#     policy: PolicyBase = IfLookupWithNoNewDataThenKeepOnlySystemIdPublicId(metadata=metadata, submission=submission)
#     policy.update()

#     assert "col1" in submission.data_tables["table1"].columns
#     assert "col2" in submission.data_tables["table1"].columns
