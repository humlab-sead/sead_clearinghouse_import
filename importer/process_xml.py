import logging
import numbers
import time
from typing import Any
from xml.sax.saxutils import escape

import numpy as np
import pandas as pd
from jinja2 import Environment, Template, select_autoescape
from loguru import logger

from importer.model import Metadata, SubmissionData
from importer.model.metadata import TableSpec

# pylint: disable=too-many-nested-blocks, too-many-statements


class XmlProcessor:
    """
    Main class that processes the Excel file and produces a corresponging XML-file.
    The format of the XML-file is conforms to clearinghouse specifications
    """

    def __init__(self, outstream, level: int = logging.WARNING, ignore_columns: list[str] = None) -> None:
        self.outstream = outstream
        self.level: int = level
        self.ignore_columns: list[str] = ignore_columns or ["date_updated"]
        self.jinja_env = Environment(autoescape=select_autoescape(["xml"]))

    def emit(self, data: str, indent: int = 0) -> None:
        self.outstream.write("{}{}\n".format("  " * indent, data))

    def emit_tag(self, tag: str, attributes: dict[str, Any] = None, indent=0, close=True) -> None:
        self.emit(
            "<{} {}{}>".format(
                tag,
                " ".join(['{}="{}"'.format(x, y) for (x, y) in (attributes or {}).items()]),
                "/" if close else "",
            ),
            indent,
        )

    def emit_close_tag(self, tag: str, indent: int) -> None:
        self.emit("</{}>".format(tag), indent)

    def camel_case_name(self, undescore_name: str) -> str:
        first, *rest = undescore_name.split("_")
        return first + "".join(word.capitalize() for word in rest)

    def process_data(
        self,
        metadata: Metadata,
        submission: SubmissionData,
        table_names: list[str],
        max_rows: int = 0,
    ) -> None:
        """
        Import assumes that all FK references points to a local "system_id" in referenced table
        All submission tables MUST have a non null "system_id"
        All submission tables MUST have a PK column with a name equal to that specified in "Tables" meta-data PK-name field
        """
        date_updated: str = time.strftime("%Y-%m-%d %H%M")
        for table_name in table_names:
            try:
                logger.info("Processing %s...", table_name)

                if table_name not in metadata:
                    raise ValueError(f"Table {table_name}: not found in metadata")

                table_spec: TableSpec = metadata[table_name]
                data_table: pd.DataFrame = submission.data_tables[table_name]

                referenced_keyset: set[str] = submission.get_referenced_keyset(metadata, table_name)
                table_namespace: str = "com.sead.database.{}".format(table_spec.java_class)

                if data_table is None:
                    continue

                self.emit('<{} length="{}">'.format(table_spec.java_class, data_table.shape[0]), 1)  # data_table.length
                # self.emit_tag(table_specification['java_class'], dict(length=data_table.shape[0]), close=False, indent=1)

                for index, record in data_table.iterrows():
                    try:
                        data_row: dict = record.to_dict()
                        public_id: int = data_row[table_spec.pk_name] if table_spec.pk_name in data_row else np.NAN

                        if np.isnan(public_id) and np.isnan(data_row["system_id"]):
                            logger.warning("Table %s: Skipping row since both CloneId and SystemID is NULL", table_name)
                            continue

                        system_id = int(data_row["system_id"] if not np.isnan(data_row["system_id"]) else public_id)

                        referenced_keyset.discard(system_id)

                        assert not (np.isnan(public_id) and np.isnan(system_id))

                        if not np.isnan(public_id):
                            public_id = int(public_id)
                            self.emit('<{} id="{}" clonedId="{}"/>'.format(table_namespace, system_id, public_id), 2)
                            continue

                        self.emit('<{} id="{}">'.format(table_namespace, system_id), 2)

                        for column_name, column_spec in table_spec.columns.items():
                            if column_name in self.ignore_columns:
                                continue

                            class_name: str = column_spec.class_name

                            if column_name not in data_row.keys():
                                logger.warning(
                                    "Table %s, FK column %s: META field name not found in submission",
                                    table_name,
                                    column_name,
                                )
                                continue

                            camel_case_column_name: str = self.camel_case_name(column_name)
                            value = data_row[column_name]
                            if not column_spec.is_fk:
                                """The value is a PK or non-FK attribte"""
                                if column_spec.is_pk:
                                    value = int(public_id) if not np.isnan(public_id) else system_id
                                elif isinstance(value, numbers.Number) and np.isnan(value):
                                    value = "NULL"
                                else:
                                    if isinstance(value, str) and any((c in "<>&") for c in value):
                                        value: str = escape(value)

                                self.emit(
                                    '<{0} class="{1}">{2}</{0}>'.format(camel_case_column_name, class_name, value),
                                    3,
                                )

                            else:
                                """The value is a FK system_id"""
                                try:
                                    fk_table_name: str = metadata[class_name].table_name
                                    if fk_table_name is None:
                                        logger.warning(
                                            "Table %s, FK column %s: unable to resolve FK class %s",
                                            table_name,
                                            column_name,
                                            class_name,
                                        )
                                        continue

                                    fk_data_table: str = submission.data_tables[fk_table_name]

                                    if np.isnan(value):
                                        self.emit(
                                            '<{} class="com.sead.database.{}" id="NULL"/>'.format(
                                                camel_case_column_name, class_name
                                            ),
                                            3,
                                        )
                                        continue

                                    fk_system_id: int = int(value)
                                    if fk_data_table is None:
                                        fk_public_id = fk_system_id
                                    else:
                                        if column_name not in fk_data_table.columns:
                                            logger.warning(
                                                "Table %s, FK column %s: FK column not found in %s, id=%s",
                                                table_name,
                                                column_name,
                                                fk_table_name,
                                                fk_system_id,
                                            )
                                            continue
                                        fk_data_row = fk_data_table.loc[(fk_data_table.system_id == fk_system_id)]
                                        if fk_data_row.empty or len(fk_data_row) != 1:
                                            fk_public_id: int = fk_system_id
                                        else:
                                            fk_public_id = fk_data_row[column_name].iloc[0]

                                    class_name = class_name.split(".")[-1]

                                    if np.isnan(fk_public_id):
                                        self.emit(
                                            '<{} class="com.sead.database.{}" id="{}"/>'.format(
                                                camel_case_column_name,
                                                class_name,
                                                fk_system_id,
                                            ),
                                            3,
                                        )
                                    else:
                                        self.emit(
                                            '<{} class="com.sead.database.{}" id="{}" clonedId="{}"/>'.format(
                                                camel_case_column_name,
                                                class_name,
                                                int(fk_system_id),
                                                int(fk_public_id),
                                            ),
                                            3,
                                        )

                                except:
                                    logger.error(
                                        "Table %s, id=%s, process failed for column %s",
                                        table_name,
                                        system_id,
                                        column_name,
                                    )
                                    raise

                        # ClonedId tag is always emitted (NULL id missing)
                        self.emit(
                            '<clonedId class="java.util.Integer">{}</clonedId>'.format(
                                "NULL" if np.isnan(public_id) else int(public_id)
                            ),
                            3,
                        )
                        self.emit_date_updated(
                            date_updated,
                            3,
                        )
                        self.emit("</{}>".format(table_namespace), 2)

                        if 0 < max_rows < index:
                            break

                    except Exception as x:
                        logger.error("CRITICAL FAILURE: Table %s %s", table_name, x)
                        raise

                if len(referenced_keyset) > 0 and max_rows == 0:
                    logger.warning(
                        "Warning: %s has %s referenced keys not found in submission",
                        table_name,
                        len(referenced_keyset),
                    )
                    for key in referenced_keyset:
                        self.emit(
                            '<com.sead.database.{} id="{}" clonedId="{}"/>'.format(
                                table_spec.java_class, int(key), int(key)
                            ),
                            2,
                        )
                self.emit("</{}>".format(table_spec.java_class), 1)

            except:
                logger.exception("CRITICAL ERROR")
                raise

    def emit_date_updated(self, date_updated: str, indent: int) -> None:
        self.emit('<dateUpdated class="java.util.Date">{}</dateUpdated>'.format(date_updated), indent)

    def process_lookups(self, metadata: Metadata, submission: SubmissionData, table_names: list[str]) -> None:
        template_str = """
        <{{class_name}} length="{{length}}">
        {% for lookup in lookups %}
            <com.sead.database.{{class_name}} id="{{lookup_id}}" clonedId="{{lookup_id}}"/>
        {% endfor %}
        </{{class_name}}>
        """
        template: Template = self.jinja_env.from_string(template_str)

        for table_name in table_names:
            referenced_keyset: set[str] = submission.get_referenced_keyset(metadata, table_name)

            if len(referenced_keyset) == 0:
                logger.info("Skipping %s: not referenced", table_name)
                continue

            class_name: str = metadata[table_name].java_class
            xml: str = template.render(
                lookups=referenced_keyset,
                class_name=class_name,
                length=len(referenced_keyset),
            )
            self.emit(xml)

    def process(
        self,
        metadata: Metadata,
        submission: SubmissionData,
        table_names: list[str] = None,
        extra_names: list[str] = None,
    ) -> None:
        tables_to_process: list[str] = submission.index_table_names if table_names is None else table_names
        extra_names: set[str] = (
            set(metadata.sead_schema.keys()) - set(submission.data_table_names) if extra_names is None else extra_names
        )

        self.emit('<?xml version="1.0" ?>')
        self.emit("<sead-data-upload>")
        self.process_lookups(metadata, submission, extra_names)
        self.process_data(metadata, submission, tables_to_process)
        self.emit("</sead-data-upload>")
