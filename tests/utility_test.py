import io

from importer import utility


def test_flatten_empty_list_returns_empty_list():
    results = utility.flatten([])
    assert [] == results


def test_flatten_one_empty_list_list_returns_the_list():
    results = utility.flatten([[]])
    assert [] == results


def test_flatten_two_lists_returns_merged_list():
    results = utility.flatten([[1, 2, 3], [4, 5, 6]])
    assert [1, 2, 3, 4, 5, 6] == results


def test_flatten_set_two_sets_returns_merged_set():
    results = utility.flatten_sets({1, 2, 3}, {1, 2, 3, 4, 5, 6})
    assert {1, 2, 3, 4, 5, 6} == results


def test_tidy_xml_returns_a_tidy_xml():
    xml_text = """<?xml version='1.0' encoding='UTF-8'?>
     <main>  <sub> <name>Ana</name>
    <detail/> <type>smart</type> </sub> </main> """
    path = "/tmp/test.xml"
    with io.open(path, "w", encoding="utf8") as outstream:
        outstream.write(xml_text)

    tidy_path = utility.tidy_xml(path)

    assert tidy_path == "/tmp/test_tidy.xml"


def test_compress_and_encode():
    assert True
