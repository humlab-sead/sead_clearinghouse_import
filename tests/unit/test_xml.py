from unittest.mock import Mock

import pandas as pd
import pytest

from importer.dispatchers.to_xml import XmlProcessor

# pylint: disable=unused-argument,redefined-outer-name


def test_emit():
    outstream = Mock()
    processor = XmlProcessor(outstream)
    processor.emit("test", 2)
    outstream.write.assert_called_once_with("    test\n")


# def test_camel_case_name():
#     outstream = Mock()
#     processor = XmlProcessor(outstream)
#     assert processor.camel_case_name('test_name') == 'testName'


@pytest.mark.skipif(True, reason="Requires lxml optional dependency")
def test_read_xml():
    source_file: str = "tests/test_data/building_dendro_reduced.xml"

    data: pd.DataFrame = pd.read_xml(source_file)
    assert data is not None
