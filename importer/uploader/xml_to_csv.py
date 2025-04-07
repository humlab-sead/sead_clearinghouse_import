# FIXME: #27 Performance problem with large files
"""
Converts the XML file to CSV files for each table type., then uploads the CSV files to the database.
"""
import os
import xml.etree.ElementTree as ET
from collections import namedtuple
from typing import Any, Iterable

import pandas as pd
from loguru import logger
from sqlalchemy.types import TEXT

from ..utility import Registry, get_connection_uri

Table = namedtuple("Table", "table_type, record_count")

Column = namedtuple("Column", "table_type, column_name, column_type")

Record = namedtuple("Record", "class_name, system_id, public_id")

RecordValue = namedtuple(
    "RecordValue",
    "class_name, system_id, public_id, column_name, column_type, fk_system_id, fk_public_id, column_value",
)

DbType = Record | Column | Table | RecordValue


class ParserRegistry(Registry):
    items: dict = {}


Parsers = ParserRegistry()


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


def load_xml(source: str) -> ET.ElementTree | ET.Element | Any:
    return ET.fromstring(source) if '<' in source else ET.parse(source).getroot()


@Parsers.register(key=Table)
def xml_to_tables(source: str) -> Iterable[Table]:
    root: ET.Element = load_xml(source)
    for table in root.iterfind("./*"):
        yield Table(table.tag, table.get('length') or "NULL")


@Parsers.register(key=RecordValue)
def xml_to_record_values(source: str) -> Iterable[RecordValue]:
    root: ET.Element = load_xml(source)
    for table in root.iterfind("./*"):
        found_record_count: int = 0
        for record in table.iterfind("./*"):
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


@Parsers.register(key=Column)
def xml_to_columns(source: str) -> Iterable[Column]:
    root: ET.Element = load_xml(source)
    for table in root.iterfind("./*"):
        found: bool = False
        for record in table.findall("./*"):

            if 'clonedId' in record.attrib:
                continue

            columns: list[ET.Element] = record.findall("./*")
            for column in columns:
                yield Column(table.tag, column.tag, column.get('class'))

            logger.debug(
                f"   --> {table.tag}: has new data, found columns {', '.join(x.tag for x in columns)} for {table.tag}"
            )
            found = True

            break

        if not found:
            logger.debug(f"   --> {table.tag}: no new data found (no data records found)")


@Parsers.register(key=Record)
def xml_to_records(source: str) -> Iterable[Record]:
    root: ET.Element | Any = load_xml(source)
    for table in root.iterfind("./*"):
        for record in table.iterfind("./*"):
            local_id: str = record.get('id')
            public_id: str = record.get('clonedId')
            if public_id is None:
                column: str = record.find('./clonedId')
                if column is not None:
                    public_id = column.text or "NULL"
            yield Record(table.tag, local_id or "NULL", public_id or "NULL")


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

    uri: str = get_connection_uri(connection)
    data: pd.DataFrame = pd.read_csv(filename, sep='\t', na_values='NULL', keep_default_na=True, dtype=str)
    data.to_sql(
        target_table,
        uri,
        schema=target_schema,
        if_exists='replace',
        index=False,
        dtype={column_name: TEXT for column_name in data.columns},
    )

    # else:
    #     with open(filename, 'r') as fp:
    #         columns: list[str] = next(fp).strip().split('\t')
    #         columns_spec: list[str] = [f"{x} text null" for x in columns]

    #         with connection.cursor() as cursor:
    #             cursor.execute(f"create table if not exists {target_schema}.{target_table} ( {','.join(columns_spec)} );")
    #             cursor.execute(f"truncate {target_schema}.{target_table}")

    #         connection.commit()

    #         with connection.cursor() as cursor:
    #             cursor.execute(f"set search_path = {target_schema}")
    #             cursor.copy_from(fp, target_table, sep='\t', null='NULL', columns=columns)

    #         connection.commit()


def xml_to_csv_to_db(connection: Any, xml_filename: str, csv_folder: str, target_schema: str) -> None:
    os.makedirs(csv_folder, exist_ok=True)
    for fn_type, fn in Parsers.items.items():
        table_name: str = f'temp_submission_upload_{fn_type.__name__.lower()}'
        csv_to_db(connection, xml_to_csv(xml_filename, csv_folder, fn, fn_type), target_schema, table_name)
