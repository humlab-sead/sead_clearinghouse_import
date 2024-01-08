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

LOOKUP_TEMPLATE: str = """
<{{class_name}} length="{{length}}">
{% for lookup_id in lookup_ids %} <com.sead.database.{{class_name}} id="{{lookup_id}}" clonedId="{{lookup_id}}"/>
{% endfor -%}
</{{class_name}}>
"""


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
        attrib_str: str = " ".join(['{}="{}"'.format(x, y) for (x, y) in (attributes or {}).items()])
        self.emit(f"<{tag} {attrib_str}{'/' if close else ''}>", indent)

    def emit_close_tag(self, tag: str, indent: int) -> None:
        self.emit(f"</{tag}>", indent)

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
        for table_name in sorted(table_names):
            try:
                logger.info(f"Processing {table_name}...")

                if table_name not in metadata:
                    raise ValueError(f"Table {table_name}: not found in metadata")

                table_spec: TableSpec = metadata[table_name]
                data_table: pd.DataFrame = submission.data_tables[table_name]

                referenced_keyset: set[str] = submission.get_referenced_keyset(metadata, table_name)
                table_namespace: str = f"com.sead.database.{table_spec.java_class}"

                if data_table is None:
                    continue

                self.emit(
                    f'<{table_spec.java_class} length="{data_table.shape[0]}">', 1
                )  # data_table.length
                # self.emit_tag(table_specification['java_class'], dict(length=data_table.shape[0]), close=False, indent=1)

                for index, record in data_table.iterrows():
                    try:
                        data_row: dict = record.to_dict()
                        public_id: int = data_row[table_spec.pk_name] if table_spec.pk_name in data_row else np.NAN

                        if np.isnan(public_id) and np.isnan(data_row["system_id"]):
                            logger.warning(f"Table {table_name}: Skipping row since both CloneId and SystemID is NULL")
                            continue

                        system_id = int(data_row["system_id"] if not np.isnan(data_row["system_id"]) else public_id)

                        referenced_keyset.discard(system_id)

                        assert not (np.isnan(public_id) and np.isnan(system_id))

                        if not np.isnan(public_id):
                            public_id = int(public_id)
                            self.emit(f'<{table_namespace} id="{system_id}" clonedId="{public_id}"/>', 2)
                            continue

                        self.emit(f'<{table_namespace} id="{system_id}">', 2)

                        for column_name, column_spec in table_spec.columns.items():
                            if column_name in self.ignore_columns:
                                continue

                            class_name: str = column_spec.class_name

                            if column_name not in data_row.keys():
                                logger.warning(
                                    f"Table {table_name}, FK column {column_name}: META field name not found in submission"
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
                                    f'<{camel_case_column_name} class="{class_name}">{value}</{camel_case_column_name}>',
                                    3,
                                )

                            else:
                                """The value is a FK system_id"""
                                try:
                                    fk_table_name: str = metadata[class_name].table_name
                                    if fk_table_name is None:
                                        logger.warning(
                                            f"Table {table_name}, FK column {column_name}: unable to resolve FK class {class_name}"
                                        )
                                        continue

                                    fk_data_table: str = submission.data_tables[fk_table_name]

                                    if np.isnan(value):
                                        self.emit(
                                            f'<{camel_case_column_name} class="com.sead.database.{class_name}" id="NULL"/>',
                                            3,
                                        )
                                        continue

                                    fk_system_id: int = int(value)
                                    if fk_data_table is None:
                                        fk_public_id = fk_system_id
                                    else:
                                        if column_name not in fk_data_table.columns:
                                            logger.warning(
                                                f"Table {table_name}, FK column {column_name}: FK column not found in {fk_table_name}, id={fk_system_id}"
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
                                            f'<{camel_case_column_name} class="com.sead.database.{class_name}" id="{fk_system_id}"/>',
                                            3,
                                        )
                                    else:
                                        self.emit(
                                            f'<{camel_case_column_name} class="com.sead.database.{class_name}" id="{int(fk_system_id)}" clonedId="{int(fk_public_id)}"/>',
                                            3,
                                        )

                                except:
                                    logger.error(
                                        f"Table {table_name}, id={system_id}, process failed for column {column_name}"
                                    )
                                    raise

                        # ClonedId tag is always emitted (NULL id missing)
                        self.emit(
                            f'<clonedId class="java.util.Integer">{"NULL" if np.isnan(public_id) else int(public_id)}</clonedId>',
                            3,
                        )
                        self.emit('<dateUpdated class="java.util.Date"/>', 3)

                        self.emit(f"</{table_namespace}>", 2)

                        if 0 < max_rows < index:
                            break

                    except Exception as x:
                        logger.error(f"CRITICAL FAILURE: Table {table_name} {x}")
                        raise

                if len(referenced_keyset) > 0 and max_rows == 0:
                    logger.warning(
                        f"Warning: {table_name} has {len(referenced_keyset)} referenced keys not found in submission"
                    )
                    for key in referenced_keyset:
                        self.emit(
                            f'<com.sead.database.{table_spec.java_class} id="{int(key)}" clonedId="{int(key)}"/>', 2
                        )
                self.emit(f"</{table_spec.java_class}>", 1)

            except:
                logger.exception("CRITICAL ERROR")
                raise

    def process_lookups(self, metadata: Metadata, submission: SubmissionData, table_names: list[str]) -> None:
        template: Template = self.jinja_env.from_string(LOOKUP_TEMPLATE)

        for table_name in table_names:
            referenced_keyset: set[str] = submission.get_referenced_keyset(metadata, table_name)

            if len(referenced_keyset) == 0:
                logger.info(f"Skipping {table_name}: not referenced")
                continue

            class_name: str = metadata[table_name].java_class
            xml: str = template.render(
                lookup_ids=referenced_keyset, class_name=class_name, length=len(referenced_keyset)
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
