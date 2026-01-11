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

from importer.utility import Registry, get_connection_uri

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


Parsers = ParserRegistry()  # pylint: disable=invalid-name


def format_value(value: str, data_type: str) -> str:
    if value is None or value == "NULL":
        return ""
    if data_type == "java.lang.String":
        return '"' + value.replace('"', '""') + '"'  # escape double quotes
    if data_type in ("java.lang.Integer", "java.lang.Long", "java.lang.Short"):
        return str(int(float(value)))
    if data_type.startswith("com.sead.database."):  # FK values
        return str(int(float(value)))
    return value


def load_xml(source: str) -> ET.ElementTree | ET.Element | Any:
    return ET.fromstring(source) if "<" in source else ET.parse(source).getroot()


class BaseParser:
    """Base class for XML parsers."""

    def parse(self, source: str) -> Iterable[DbType]:
        raise NotImplementedError("Subclasses must implement parse method")


@Parsers.register(key="table", fn_type=Table)
class TableParser(BaseParser):
    """Parses table elements from XML."""

    def parse(self, source: str) -> Iterable[DbType]:
        root: ET.Element = load_xml(source)  # type: ignore
        for table in root.iterfind("./*"):
            yield Table(table.tag, table.get("length") or "NULL")


@Parsers.register(key="record_value", fn_type=RecordValue)
class RecordValueParser(BaseParser):
    """Parses record value elements from XML."""

    def parse(self, source: str) -> Iterable[DbType]:
        root: ET.Element = load_xml(source)  # type: ignore
        for table in root.iterfind("./*"):
            found_record_count: int = 0
            for record in table.iterfind("./*"):
                has_values: bool = False
                for column in record.findall("./*"):
                    has_values = True
                    yield RecordValue(
                        table.tag,
                        record.get("id") or "NULL",  # system_id
                        record.get("clonedId") or "NULL",  # public_id
                        column.tag,  # column_name
                        column.get("class") or "NULL",  # column_type
                        column.get("id") or "NULL",  # fk_system_id
                        column.get("clonedId") or "NULL",  # fk_public_id
                        format_value(column.text, column.get("class")) or "NULL",  # type: ignore
                    )
                if has_values:
                    found_record_count += 1


@Parsers.register(key="column", fn_type=Column)
class ColumnParser(BaseParser):
    """Parses column elements from XML."""

    def parse(self, source: str) -> Iterable[DbType]:
        root: ET.Element = load_xml(source)  # type: ignore
        for table in root.iterfind("./*"):
            found: bool = False
            for record in table.findall("./*"):

                if "clonedId" in record.attrib:
                    continue

                columns: list[ET.Element] = record.findall("./*")
                for column in columns:
                    yield Column(table.tag, column.tag, column.get("class"))

                logger.debug(
                    f"   --> {table.tag}: has new data, found columns {', '.join(x.tag for x in columns)} for {table.tag}"
                )
                found = True

                break

            if not found:
                logger.debug(f"   --> {table.tag}: no new data found (no data records found)")


@Parsers.register(key="record", fn_type=Record)
class RecordParser(BaseParser):
    """Parses record elements from XML."""

    def parse(self, source: str) -> Iterable[DbType]:
        root: ET.Element | Any = load_xml(source)
        for table in root.iterfind("./*"):
            for record in table.iterfind("./*"):
                local_id: str | None = record.get("id")
                public_id: str | None = record.get("clonedId")
                if public_id is None:
                    column: ET.Element | None = record.find("./clonedId")
                    if column is not None:
                        public_id = column.text or "NULL"
                yield Record(table.tag, local_id or "NULL", public_id or "NULL")


def xml_to_csv(xml_filename: str, csv_folder: str, parser_cls: BaseParser, iter_type: DbType) -> str:
    """Converts XML file to CSV file for the given record type."""
    basename: str = os.path.splitext(os.path.basename(xml_filename))[0]
    filename: str = os.path.join(csv_folder, f"{basename}_{iter_type.__name__.lower()}s.csv")  # type: ignore
    with open(filename, "w", encoding="utf-8") as f:
        f.write("\t".join(iter_type._fields) + "\n")
        for record in parser_cls().parse(xml_filename):  # type: ignore
            f.write("\t".join("" if x is None else x for x in record) + "\n")
    return filename


def csv_to_db(connection: Any, filename: str, target_schema: str, target_table: str) -> None:
    """Using the csv files created by to_csv, import the data into the PostgreSQL database using psycopg"""

    uri: str = get_connection_uri(connection)
    data: pd.DataFrame = pd.read_csv(filename, sep="\t", na_values="NULL", keep_default_na=True, dtype=str)
    data.to_sql(
        target_table,
        uri,
        schema=target_schema,
        if_exists="replace",
        index=False,
        dtype={column_name: TEXT for column_name in data.columns},
    )

    # else:
    #     with open(filename, 'r') as fp:
    #         columns: list[str] = next(fp).strip().split('\t')
    #         columns_spec: list[str] = [f"{x} text null" for x in columns]

    #         with connection.cursor() as cursor:
    #             cursor.execute(f"create table if not exists {target_schema}.{
    #                   target_table} ( {','.join(columns_spec)} );")
    #             cursor.execute(f"truncate {target_schema}.{target_table}")

    #         connection.commit()

    #         with connection.cursor() as cursor:
    #             cursor.execute(f"set search_path = {target_schema}")
    #             cursor.copy_from(fp, target_table, sep='\t', null='NULL', columns=columns)

    #         connection.commit()


def xml_to_csv_to_db(connection: Any, xml_filename: str, csv_folder: str, target_schema: str) -> None:
    os.makedirs(csv_folder, exist_ok=True)
    for key, parser_cls in Parsers.items.items():
        table_name: str = f"temp_submission_upload_{key}"
        filename: str = xml_to_csv(xml_filename, csv_folder, parser_cls, key)
        csv_to_db(connection, filename, target_schema, table_name)
