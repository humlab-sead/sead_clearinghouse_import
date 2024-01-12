# FIXME: #27 Performance problem with large files
"""
Converts the XML file to CSV files for each table type., then uploads the CSV files to the database.
"""
import os
import xml.etree.ElementTree as ET
from collections import namedtuple
from typing import Any, Iterable

from importer.utility import Registry

Table = namedtuple("Table", "table_type, record_count")

Column = namedtuple("Column", "table_type, column_name, column_type")

Record = namedtuple("Record", "class_name, system_id, public_id")

RecordValue = namedtuple(
    "RecordValue",
    "class_name, system_id, public_id, column_name, column_type, fk_system_id, fk_public_id, column_value",
)

DbType = Record | Column | Table | RecordValue

ParserRegistry = Registry()


def format_value(value: str, data_type: str) -> str:
    if value is None or value == 'NULL':
        return ''
    if data_type == 'java.lang.String':
        return '"' + value.replace('"', '""') + '"'  # escape double quotes
    if data_type in ('java.lang.Integer', 'java.lang.Long', 'java.lang.Short'):
        return str(int(float(value)))
    if data_type.startswith('com.sead.database.'):  # FK values
        return str(int(float(value)))
    return value


@ParserRegistry.register(key=Table)
def xml_to_tables(source: str) -> Iterable[Table]:
    tree: ET.ElementTree = ET.parse(source)
    root: ET.Element | Any = tree.getroot()
    for table in root.iterfind("./*"):
        yield Table(table.tag, table.get('length') or "NULL")


@ParserRegistry.register(key=RecordValue)
def xml_to_record_values(source: str) -> Iterable[RecordValue]:
    tree: ET.ElementTree = ET.parse(source)
    root: ET.Element | Any = tree.getroot()
    for table in root.iterfind("./*"):
        # record_count: int = int(table.get('length'))
        found_record_count: int = 0
        for record in table.findall("./*"):
            has_values: bool = False
            for column in record.findall("./*"):
                has_values = True
                yield RecordValue(
                    table.tag,
                    record.get('id') or "NULL",  # system_id
                    record.get('clonedId') or "NULL",  # public_id
                    column.tag,  # column_name
                    column.get('class') or "NULL",  # column_type
                    column.get('id') or "NULL",  # fk_system_id
                    column.get('clonedId') or "NULL",  # fk_public_id
                    format_value(column.text, column.get('class')) or "NULL",
                )
            if has_values:
                found_record_count += 1


@ParserRegistry.register(key=Column)
def xml_to_columns(source: str) -> Iterable[Column]:
    tree: ET.ElementTree = ET.parse(source)
    root: ET.Element | Any = tree.getroot()
    for table in root.iterfind("./*"):
        # get first child to table
        record: ET.Element | Any = table.find("./*")
        for column in record.findall("./*"):
            yield Column(
                table.tag,
                column.tag,
                column.get('class'),
            )


@ParserRegistry.register(key=Record)
def xml_to_records(source: str) -> Iterable[Record]:
    tree: ET.ElementTree = ET.parse(source)
    root: ET.Element | Any = tree.getroot()
    for table in root.iterfind("./*"):
        for record in table.findall("./*"):
            local_id: str = record.get('id') or "NULL"
            public_id: str = record.get('clonedId') or "NULL"
            if public_id is None:
                column = record.find('./clonedId')
                if column is not None:
                    public_id = column.text or "NULL"
            yield Record(table.tag, local_id, public_id)


def xml_to_csv(xml_filename: str, csv_folder: str, iter_fn: Iterable[Any], iter_type: DbType) -> str:
    basename: str = os.path.splitext(os.path.basename(xml_filename))[0]
    filename: str = os.path.join(csv_folder, f"{basename}_{iter_type.__name__.lower()}s.csv")
    with open(filename, 'w') as f:
        f.write('\t'.join(iter_type._fields) + '\n')
        for record in iter_fn(xml_filename):
            f.write('\t'.join('' if x is None else x for x in record) + '\n')
    return filename


def csv_to_db(connection: Any, filename: str, target_schema: str, target_table: str) -> None:
    """Using the csv files created by to_csv, import the data into the PostgreSQL database using psycopg2"""
    with open(filename, 'r') as fp:
        columns: list[str] = next(fp).strip().split('\t')
        columns_spec: list[str] = [f"{x} text null" for x in columns]

        with connection.cursor() as cursor:
            cursor.execute(f"create table of not exists {target_schema}.{target_table} ( {','.join(columns_spec)} );")
            cursor.execute(f"truncate {target_schema}.{target_table}")

        connection.commit()

        with connection.cursor() as cursor:
            cursor.execute(f"set search_path = {target_schema}")
            cursor.copy_from(fp, target_table, sep='\t', null='NULL', columns=columns)

        connection.commit()


def xml_to_csv_to_db(connection: Any, xml_filename: str, csv_folder: str, target_schema: str) -> None:
    for fn_type, fn in ParserRegistry.items.items():
        table_name: str = f'temp_submission_upload_{fn_type.__name__.lower()}'
        csv_to_db(connection, xml_to_csv(xml_filename, csv_folder, fn, fn_type), target_schema, table_name)
