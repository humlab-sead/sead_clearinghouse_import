from unittest.mock import Mock

import pandas as pd

from importer.dispatchers.to_xml import XmlProcessor

# pylint: disable=unused-argument,redefined-outer-name


def test_emit():
    outstream = Mock()
    processor = XmlProcessor(outstream)
    processor.emit('test', 2)
    outstream.write.assert_called_once_with('    test\n')


# def test_camel_case_name():
#     outstream = Mock()
#     processor = XmlProcessor(outstream)
#     assert processor.camel_case_name('test_name') == 'testName'


def test_read_xml():
    source_file: str = 'tests/test_data/building_dendro_reduced.xml'

    data: pd.DataFrame = pd.read_xml(source_file)
    assert data is not None


# def test_process_data():
#     outstream = Mock()
#     processor = XmlProcessor(outstream)
#     metadata = Metadata()
#     submission = SubmissionData()
#     table_names = ['table1', 'table2']
#     max_rows = 10

#     table_spec = TableSpec()
#     table_spec.java_class = 'TestClass'
#     table_spec.pk_name = 'pk'
#     table_spec.columns = {'column1': Mock(), 'column2': Mock()}

#     metadata['table1'] = table_spec
#     metadata['table2'] = table_spec

#     submission.data_tables['table1'] = Mock()
#     submission.data_tables['table2'] = Mock()

#     processor.process_data(metadata, submission, table_names, max_rows)

#     # Add assertions here to verify the expected behavior
