from importer.uploader.xml_to_to_csv import (
    Column,
    Record,
    RecordValue,
    Table,
    xml_to_columns,
    xml_to_record_values,
    xml_to_records,
    xml_to_tables,
)

XML_SNIPPET: str = """<?xml version="1.0" ?>
<sead-data-upload>

<TblAbundanceElements length="1">
 <com.sead.database.TblAbundanceElements id="44" clonedId="44"/>
</TblAbundanceElements>

<TblBiblio length="2">
 <com.sead.database.TblBiblio id="352" clonedId="352"/>
 <com.sead.database.TblBiblio id="129" clonedId="129"/>
</TblBiblio>

<TblTaxaTreeMaster length="1">
 <com.sead.database.TblTaxaTreeMaster id="18197" clonedId="18197"/>
</TblTaxaTreeMaster>
<TblAbundances length="2">
    <com.sead.database.TblAbundances id="3930">
      <abundanceId class="java.lang.Integer">3930</abundanceId>
      <taxonId class="com.sead.database.TblTaxaTreeMaster" id="18197" clonedId="18197"/>
      <analysisEntityId class="com.sead.database.TblAnalysisEntities" id="4191"/>
      <abundanceElementId class="com.sead.database.TblAbundanceElements" id="44" clonedId="44"/>
      <abundance class="java.lang.Integer">1</abundance>
      <clonedId class="java.util.Integer">NULL</clonedId>
      <dateUpdated class="java.util.Date"/>
    </com.sead.database.TblAbundances>
    <com.sead.database.TblAbundances id="3931">
      <abundanceId class="java.lang.Integer">3931</abundanceId>
      <taxonId class="com.sead.database.TblTaxaTreeMaster" id="18197" clonedId="18197"/>
      <analysisEntityId class="com.sead.database.TblAnalysisEntities" id="4192"/>
      <abundanceElementId class="com.sead.database.TblAbundanceElements" id="44" clonedId="44"/>
      <abundance class="java.lang.Integer">1</abundance>
      <clonedId class="java.util.Integer">12</clonedId>
      <dateUpdated class="java.util.Date"/>
    </com.sead.database.TblAbundances>
  </TblAbundances>
</sead-data-upload>
"""


def test_xml_to_records_by_str():

    """Test that tables are correctly extracted from XML string"""
    tables: list[Table] = list(xml_to_tables(XML_SNIPPET))
    assert len(tables) == 4, "No tables found"
    assert { x.table_type for x in tables } == {'TblAbundanceElements', 'TblBiblio', 'TblTaxaTreeMaster', 'TblAbundances'}
    assert Table('TblAbundanceElements', '1') in tables, "Table not found"
    assert Table('TblBiblio', '2') in tables, "Table not found"
    assert Table('TblTaxaTreeMaster', '1') in tables, "Table not found"
    assert Table('TblAbundances', '2') in tables, "Table not found"

    """Test that columns are correctly extracted from XML string"""
    columns: list[Column] = list(xml_to_columns(XML_SNIPPET))
    assert len(columns) == 7
    assert { x.column_name for x in columns } == {'abundanceId', 'taxonId', 'analysisEntityId', 'abundanceElementId', 'abundance', 'clonedId', 'dateUpdated'}
    assert Column('TblAbundances', 'abundanceId', 'java.lang.Integer') in columns, "Column not found"
    assert Column('TblAbundances', 'taxonId', 'com.sead.database.TblTaxaTreeMaster') in columns, "Column not found"
        

    """Test that records are correctly extracted from XML string"""
    records: list[Record] = list(xml_to_records(XML_SNIPPET))
    abundances: list[Record] = [r for r in records if r.class_name == 'TblAbundances']
    assert len(abundances) == 2
    assert {r.public_id for r in abundances} == {'NULL', '12'}, "Incorrect public IDs found"

    references: list[Record] = [r for r in records if r.class_name == 'TblBiblio']
    assert len(references) == 2
    assert Record('TblBiblio', '352', '352') in references
    assert Record('TblBiblio', '129', '129') in references
    assert Record('TblAbundances', '3930', 'NULL') in abundances
    assert Record('TblAbundances', '3931', '12') in abundances

    """Test that record values are correctly extracted from XML string"""
    record_values: list[RecordValue] = list(xml_to_record_values(XML_SNIPPET))
    assert len(record_values) > 0, "No records found"
    assert len(record_values) == 14
    assert (
        RecordValue('TblAbundances', '3930', 'NULL', 'abundanceId', 'java.lang.Integer', 'NULL', 'NULL', '3930')
        in record_values
    )
    assert (
        RecordValue(
            'TblAbundances', '3930', 'NULL', 'taxonId', 'com.sead.database.TblTaxaTreeMaster', '18197', '18197', 'NULL'
        )
        in record_values
    )
    assert (
        RecordValue(
            'TblAbundances',
            '3930',
            'NULL',
            'analysisEntityId',
            'com.sead.database.TblAnalysisEntities',
            '4191',
            'NULL',
            'NULL',
        )
        in record_values
    )
    assert (
        RecordValue(
            'TblAbundances',
            '3931',
            'NULL',
            'clonedId',
            'java.util.Integer',
            'NULL',
            'NULL',
            '12',
        )
        in record_values
    )
    # RecordValue(class_name='TblAbundances', system_id='3931', public_id='NULL', column_name='clonedId', column_type='java.util.Integer', fk_system_id='NULL', fk_public_id='NULL', column_value='12')

def test_xml_to_records_by_file():
    test_xml_path: str = 'tests/test_data/building_dendro_reduced.xml'

    tables: list[Table] = list(xml_to_tables(test_xml_path))
    assert len(tables) > 0, "No tables found"
    assert len(tables) == 43, "Incorrect number of tables found"
    assert Table('TblAbundanceElements', '1') in tables, "Table not found"
    assert Table('TblBiblio', '2') in tables, "Table not found"
    assert Table('TblTaxaTreeMaster', '1') in tables, "Table not found"
    assert Table('TblAbundances', '2') in tables, "Table not found"

    columns: list[Column] = list(xml_to_columns(test_xml_path))
    assert len(columns) > 0, "No columns found"
    assert len(columns) == 7, "Incorrect number of columns found"
    assert Column('TblAbundances', 'abundanceId', 'java.lang.Integer') in columns, "Column not found"
    assert Column('TblAbundances', 'taxonId', 'com.sead.database.TblTaxaTreeMaster') in columns, "Column not found"
    assert (
        Column('TblAbundances', 'analysisEntityId', 'com.sead.database.TblAnalysisEntities') in columns
    ), "Column not found"
    assert (
        Column('TblAbundances', 'abundanceElementId', 'com.sead.database.TblAbundanceElements') in columns
    ), "Column not found"
    assert Column('TblAbundances', 'abundance', 'java.lang.Integer') in columns, "Column not found"
    assert Column('TblAbundances', 'clonedId', 'java.util.Integer') in columns, "Column not found"
    assert Column('TblAbundances', 'dateUpdated', 'java.util.Date') in columns, "Column not found"

    records: list[Record] = list(xml_to_records(test_xml_path))

    assert len(records) > 0, "No records found"
    assert len(records) == 208

    for record in records:
        assert isinstance(record, Record), "Record is not an instance of the Record class"
        assert record.system_id.isnumeric(), "Local ID is None"
        assert record.public_id.isnumeric() or record.public_id == "NULL", "Public ID is None"

    abundances = [r for r in records if r.class_name == 'TblAbundances']
    assert len(abundances) == 2
    assert Record('TblAbundances', '3930', 'NULL') in abundances
    assert Record('TblAbundances', '3931', 'NULL') in abundances

    references = [r for r in records if r.class_name == 'TblBiblio']
    assert len(references) == 2
    assert Record('TblBiblio', '352', '352') in references
    assert Record('TblBiblio', '129', '129') in references

    record_values: list[RecordValue] = list(xml_to_records(test_xml_path))
    assert len(record_values) > 0, "No records found"
    assert len(record_values) == 14
    assert (
        RecordValue('TblAbundances', '3930', 'NULL', 'abundanceId', 'java.lang.Integer', 'NULL', 'NULL', '3930')
        in record_values
    )
    assert (
        RecordValue(
            'TblAbundances', '3930', 'NULL', 'taxonId', 'com.sead.database.TblTaxaTreeMaster', '18197', '18197', 'NULL'
        )
        in record_values
    )
    assert (
        RecordValue(
            'TblAbundances',
            '3930',
            'NULL',
            'analysisEntityId',
            'com.sead.database.TblAnalysisEntities',
            '4191',
            '4191',
            'NULL',
        )
        in record_values
    )
