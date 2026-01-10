import pandas as pd
import pytest

from importer.configuration.config import Config
from importer.metadata import SeadSchema
from importer.utility import create_db_uri

# pylint: disable=redefined-outer-name,no-member


# def test_load_sql_from_file():
#     sql: str = load_sql_from_file("source_tables")
#     assert "select" in sql.lower()
#     sql: str = load_sql_from_file("source_columns")
#     assert "select" in sql.lower()


def test_metadata_load_metadata(cfg: Config, schema: SeadSchema):
    schema: SeadSchema = SeadSchema(create_db_uri(**cfg.get("options:database")))
    assert isinstance(schema.source_tables, pd.DataFrame)
    assert isinstance(schema.source_columns, pd.DataFrame)
    assert isinstance(schema.sead_schema, dict)
    assert isinstance(schema._foreign_keys, pd.DataFrame)


def test_tables_specifications(schema: SeadSchema):
    assert isinstance(schema.sead_schema, dict)

    assert len(schema.source_tables) == len(schema.sead_schema)
    assert set(schema.source_tables.columns) - {"columns", "is_new"} == set(
        schema.sead_schema["tbl_sites"].keys()
    ) - {"columns", "is_new"}

    assert "columns" in schema.sead_schema["tbl_sites"].keys()
    assert "site_id" in schema.sead_schema["tbl_sites"].columns.keys()
    assert len(schema.sead_schema["tbl_sites"].columns) == len(
        schema.source_columns[schema.source_columns.table_name == "tbl_sites"]
    )
    assert schema.sead_schema["tbl_sites"].columns["site_id"].is_pk is True
    assert schema.sead_schema["tbl_sites"].columns["site_name"].is_pk is False
    assert schema.sead_schema["tbl_locations"].columns["location_type_id"].is_fk is True
    assert schema.sead_schema["tbl_locations"].columns["location_id"].is_fk is False
    assert schema.sead_schema["tbl_locations"].columns["location_id"].is_pk is True


def test_is_pk(schema: SeadSchema):
    assert schema.is_pk("tbl_sites", "site_id") is True
    assert schema.is_pk("tbl_sites", "site_name") is False
    assert schema.is_pk("tbl_locations", "location_type_id") is False
    assert schema.is_pk("tbl_locations", "location_id") is True


def test_is_fk(schema: SeadSchema):
    assert schema.is_fk("tbl_sites", "site_id") is False
    assert schema.is_fk("tbl_sites", "site_name") is False
    assert schema.is_fk("tbl_locations", "location_type_id") is True
    assert schema.is_fk("tbl_locations", "location_id") is False


def test_get_tablenames_referencing(cfg: Config):
    schema: SeadSchema = SeadSchema(create_db_uri(**cfg.get("options:database")))

    assert set(schema.get_tablenames_referencing("tbl_sites")) == {
        "tbl_sample_groups",
        "tbl_site_images",
        "tbl_site_locations",
        "tbl_site_natgridrefs",
        "tbl_site_other_records",
        "tbl_site_preservation_status",
        "tbl_site_references",
    }


@pytest.mark.parametrize(
    "values",
    [
        ("tbl_abundances", "abundance_element_id", "tbl_abundance_elements", "TblAbundanceElements"),
        ("tbl_analysis_entities", "physical_sample_id", "tbl_physical_samples", "TblPhysicalSamples"),
        ("tbl_datasets", "master_set_id", "tbl_dataset_masters", "TblDatasetMasters"),
        ("tbl_datasets", "project_id", "tbl_projects", "TblProjects"),
    ],
)
def test_foreign_keys(schema: SeadSchema, values: list[str]):
    assert isinstance(schema._foreign_keys, pd.DataFrame)
    assert len(schema._foreign_keys) > 0
    assert (schema._foreign_keys == values).all(axis=1).any()
