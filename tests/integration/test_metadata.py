import pandas as pd
import pytest

from importer.configuration.config import Config
from importer.metadata import SchemaService, SeadSchema

# pylint: disable=redefined-outer-name,no-member


# def test_load_sql_from_file():
#     sql: str = load_sql_from_file("source_tables")
#     assert "select" in sql.lower()
#     sql: str = load_sql_from_file("source_columns")
#     assert "select" in sql.lower()


def test_metadata_load_metadata(cfg: Config, schema: SeadSchema):
    assert isinstance(schema.source_tables, pd.DataFrame)
    assert isinstance(schema.source_columns, pd.DataFrame)
    assert isinstance(schema._tables, dict)
    assert isinstance(schema._foreign_keys, pd.DataFrame)


def test_tables_specifications(schema: SeadSchema):
    assert isinstance(schema._tables, dict)

    assert len(schema.source_tables) == len(schema)
    assert set(schema.source_tables.columns) - {"columns", "is_new"} == set(schema["tbl_sites"].keys()) - {
        "columns",
        "is_new",
    }

    assert "columns" in schema["tbl_sites"].keys()
    assert "site_id" in schema["tbl_sites"].columns.keys()
    assert len(schema["tbl_sites"].columns) == len(
        schema.source_columns[schema.source_columns.table_name == "tbl_sites"]
    )
    assert schema["tbl_sites"].columns["site_id"].is_pk is True
    assert schema["tbl_sites"].columns["site_name"].is_pk is False
    assert schema["tbl_locations"].columns["location_type_id"].is_fk is True
    assert schema["tbl_locations"].columns["location_id"].is_fk is False
    assert schema["tbl_locations"].columns["location_id"].is_pk is True


def test_is_pk(service: SchemaService, cfg: Config):
    schema: SeadSchema = service.load()
    assert schema.is_pk("tbl_sites", "site_id") is True
    assert schema.is_pk("tbl_sites", "site_name") is False
    assert schema.is_pk("tbl_locations", "location_type_id") is False
    assert schema.is_pk("tbl_locations", "location_id") is True


def test_is_fk(schema: SeadSchema):
    assert schema.is_fk("tbl_sites", "site_id") is False
    assert schema.is_fk("tbl_sites", "site_name") is False
    assert schema.is_fk("tbl_locations", "location_type_id") is True
    assert schema.is_fk("tbl_locations", "location_id") is False


def test_get_tablenames_referencing(service: SchemaService, cfg: Config):
    schema: SeadSchema = service.load()

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
def test_foreign_keys(service: SchemaService, values: list[str]):
    schema: SeadSchema = service.load()
    assert isinstance(schema._foreign_keys, pd.DataFrame)
    assert len(schema._foreign_keys) > 0
    assert (schema._foreign_keys == values).all(axis=1).any()
