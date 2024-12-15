import io
from typing import Any

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


def test_recursive_update():
    d1: dict[str, Any] = {'a': 1, 'b': 2}
    d2: dict[str, Any] = {'b': 3, 'c': 4}
    result: dict[str, Any] = utility.recursive_update(d1, d2)
    assert result == {'a': 1, 'b': 3, 'c': 4}

    # def test_recursive_update_nested():
    d1 = {'a': 1, 'b': {'x': 10, 'y': 20}}
    d2 = {'b': {'y': 30, 'z': 40}, 'c': 4}
    result = utility.recursive_update(d1, d2)
    assert result == {'a': 1, 'b': {'x': 10, 'y': 30, 'z': 40}, 'c': 4}

    # def test_recursive_update_empty_d2():
    d1 = {'a': 1, 'b': 2}
    d2 = {}
    result = utility.recursive_update(d1, d2)
    assert result == {'a': 1, 'b': 2}

    # def test_recursive_update_empty_d1():
    d1 = {}
    d2 = {'a': 1, 'b': 2}
    result = utility.recursive_update(d1, d2)
    assert result == {'a': 1, 'b': 2}

    # def test_recursive_update_overwrite_with_non_dict():
    d1 = {'a': {'x': 10}}
    d2 = {'a': 1}
    result = utility.recursive_update(d1, d2)
    assert result == {'a': 1}
    d1 = {'a': 1, 'b': 2}
    d2 = {'b': 3, 'c': 4}
    result = utility.recursive_update(d1, d2)
    assert result == {'a': 1, 'b': 3, 'c': 4}

    # def test_recursive_update_nested():
    d1 = {'a': 1, 'b': {'x': 10, 'y': 20}}
    d2 = {'b': {'y': 30, 'z': 40}, 'c': 4}
    result = utility.recursive_update(d1, d2)
    assert result == {'a': 1, 'b': {'x': 10, 'y': 30, 'z': 40}, 'c': 4}

    # def test_recursive_update_empty_d2():
    d1 = {'a': 1, 'b': 2}
    d2 = {}
    result = utility.recursive_update(d1, d2)
    assert result == {'a': 1, 'b': 2}

    # def test_recursive_update_empty_d1():
    d1 = {}
    d2 = {'a': 1, 'b': 2}
    result = utility.recursive_update(d1, d2)
    assert result == {'a': 1, 'b': 2}

    # def test_recursive_update_overwrite_with_non_dict():
    d1 = {'a': {'x': 10}}
    d2 = {'a': 1}
    result = utility.recursive_update(d1, d2)
    assert result == {'a': 1}


def test_recursive_filter_dict_keep_mode():
    d = {
        'a': 1,
        'b': {'x': 10, 'y': 20},
        'c': {'z': 30, 'w': {'u': 40, 'v': 50}},
    }
    filter_keys = {'a', 'b', 'z', 'u'}
    result = utility.recursive_filter_dict(d, filter_keys, 'keep')
    # 'z', 'u' in 'c' removed since 'c' is not in filter_keys
    expected = {'a': 1, 'b': {}}
    assert result == expected


def test_recursive_filter_dict_exclude_mode():
    d = {
        'a': 1,
        'b': {'x': 10, 'y': 20},
        'c': {'z': 30, 'w': {'u': 40, 'v': 50}},
    }
    filter_keys = {'a', 'b', 'z', 'u'}
    result = utility.recursive_filter_dict(d, filter_keys, 'exclude')
    expected = {
        'c': {'w': {'v': 50}},
    }

    assert result == expected


def test_recursive_filter_dict_empty_filter_keys():
    d = {
        'a': 1,
        'b': {'x': 10, 'y': 20},
        'c': {'z': 30, 'w': {'u': 40, 'v': 50}},
    }
    filter_keys = set()
    result = utility.recursive_filter_dict(d, filter_keys, 'keep')
    expected = {}
    assert result == expected


def test_recursive_filter_dict_non_dict_input():
    d = "not a dict"
    filter_keys = {'a', 'b'}
    result = utility.recursive_filter_dict(d, filter_keys, 'keep')
    assert result == d


def test_pascal_to_snake_case():
    assert utility.pascal_to_snake_case("PascalCase") == "pascal_case"
    assert utility.pascal_to_snake_case("TestString") == "test_string"
    assert utility.pascal_to_snake_case("AnotherTest") == "another_test"
    assert utility.pascal_to_snake_case("SimpleTest") == "simple_test"
    assert utility.pascal_to_snake_case("ThisIsATest") == "this_is_a_test"
    assert utility.pascal_to_snake_case("Test") == "test"
    assert utility.pascal_to_snake_case("PascalCaseTest") == "pascal_case_test"
    assert utility.pascal_to_snake_case("PascalCase123") == "pascal_case123"
    assert utility.pascal_to_snake_case("PascalCaseWithNumbers123") == "pascal_case_with_numbers123"
    assert utility.pascal_to_snake_case("PascalCaseWith123Numbers") == "pascal_case_with123_numbers"


import io
from typing import Any

from importer import utility
import pandas as pd
from unittest.mock import patch


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


def test_recursive_update():
    d1: dict[str, Any] = {'a': 1, 'b': 2}
    d2: dict[str, Any] = {'b': 3, 'c': 4}
    result: dict[str, Any] = utility.recursive_update(d1, d2)
    assert result == {'a': 1, 'b': 3, 'c': 4}

    # def test_recursive_update_nested():
    d1 = {'a': 1, 'b': {'x': 10, 'y': 20}}
    d2 = {'b': {'y': 30, 'z': 40}, 'c': 4}
    result = utility.recursive_update(d1, d2)
    assert result == {'a': 1, 'b': {'x': 10, 'y': 30, 'z': 40}, 'c': 4}

    # def test_recursive_update_empty_d2():
    d1 = {'a': 1, 'b': 2}
    d2 = {}
    result = utility.recursive_update(d1, d2)
    assert result == {'a': 1, 'b': 2}

    # def test_recursive_update_empty_d1():
    d1 = {}
    d2 = {'a': 1, 'b': 2}
    result = utility.recursive_update(d1, d2)
    assert result == {'a': 1, 'b': 2}

    # def test_recursive_update_overwrite_with_non_dict():
    d1 = {'a': {'x': 10}}
    d2 = {'a': 1}
    result = utility.recursive_update(d1, d2)
    assert result == {'a': 1}
    d1 = {'a': 1, 'b': 2}
    d2 = {'b': 3, 'c': 4}
    result = utility.recursive_update(d1, d2)
    assert result == {'a': 1, 'b': 3, 'c': 4}

    # def test_recursive_update_nested():
    d1 = {'a': 1, 'b': {'x': 10, 'y': 20}}
    d2 = {'b': {'y': 30, 'z': 40}, 'c': 4}
    result = utility.recursive_update(d1, d2)
    assert result == {'a': 1, 'b': {'x': 10, 'y': 30, 'z': 40}, 'c': 4}

    # def test_recursive_update_empty_d2():
    d1 = {'a': 1, 'b': 2}
    d2 = {}
    result = utility.recursive_update(d1, d2)
    assert result == {'a': 1, 'b': 2}

    # def test_recursive_update_empty_d1():
    d1 = {}
    d2 = {'a': 1, 'b': 2}
    result = utility.recursive_update(d1, d2)
    assert result == {'a': 1, 'b': 2}

    # def test_recursive_update_overwrite_with_non_dict():
    d1 = {'a': {'x': 10}}
    d2 = {'a': 1}
    result = utility.recursive_update(d1, d2)
    assert result == {'a': 1}


def test_recursive_filter_dict_keep_mode():
    d = {
        'a': 1,
        'b': {'x': 10, 'y': 20},
        'c': {'z': 30, 'w': {'u': 40, 'v': 50}},
    }
    filter_keys = {'a', 'b', 'z', 'u'}
    result = utility.recursive_filter_dict(d, filter_keys, 'keep')
    # 'z', 'u' in 'c' removed since 'c' is not in filter_keys
    expected = {'a': 1, 'b': {}}
    assert result == expected


def test_recursive_filter_dict_exclude_mode():
    d = {
        'a': 1,
        'b': {'x': 10, 'y': 20},
        'c': {'z': 30, 'w': {'u': 40, 'v': 50}},
    }
    filter_keys = {'a', 'b', 'z', 'u'}
    result = utility.recursive_filter_dict(d, filter_keys, 'exclude')
    expected = {
        'c': {'w': {'v': 50}},
    }

    assert result == expected


def test_recursive_filter_dict_empty_filter_keys():
    d = {
        'a': 1,
        'b': {'x': 10, 'y': 20},
        'c': {'z': 30, 'w': {'u': 40, 'v': 50}},
    }
    filter_keys = set()
    result = utility.recursive_filter_dict(d, filter_keys, 'keep')
    expected = {}
    assert result == expected


def test_recursive_filter_dict_non_dict_input():
    d = "not a dict"
    filter_keys = {'a', 'b'}
    result = utility.recursive_filter_dict(d, filter_keys, 'keep')
    assert result == d


def test_pascal_to_snake_case():
    assert utility.pascal_to_snake_case("PascalCase") == "pascal_case"
    assert utility.pascal_to_snake_case("TestString") == "test_string"
    assert utility.pascal_to_snake_case("AnotherTest") == "another_test"
    assert utility.pascal_to_snake_case("SimpleTest") == "simple_test"
    assert utility.pascal_to_snake_case("ThisIsATest") == "this_is_a_test"
    assert utility.pascal_to_snake_case("Test") == "test"
    assert utility.pascal_to_snake_case("PascalCaseTest") == "pascal_case_test"
    assert utility.pascal_to_snake_case("PascalCase123") == "pascal_case123"
    assert utility.pascal_to_snake_case("PascalCaseWithNumbers123") == "pascal_case_with_numbers123"
    assert utility.pascal_to_snake_case("PascalCaseWith123Numbers") == "pascal_case_with123_numbers"


def test_load_sead_columns():
    db_uri = "postgresql://user@localhost:5432/dbname"
    mock_data = pd.DataFrame(
        {'table_name': ['table1', 'table1', 'table2'], 'column_name': ['col1', 'col2', 'col3'], 'position': [1, 2, 1]}
    )

    with patch('importer.utility.load_sead_data', return_value=mock_data):
        result = utility.load_sead_columns(db_uri)
        assert result.equals(mock_data)

    # def test_load_sead_columns_with_ignore_columns():
    ignore_columns: list[str] = ['col2']
    with patch('importer.utility.load_sead_data', return_value=mock_data):
        result = utility.load_sead_columns(db_uri, ignore_columns)
        expected_data = mock_data[mock_data['column_name'] != 'col2']
        assert result.equals(expected_data)

    # def test_load_sead_columns_with_empty_ignore_columns():
    ignore_columns = []
    with patch('importer.utility.load_sead_data', return_value=mock_data):
        result = utility.load_sead_columns(db_uri, ignore_columns)
        assert result.equals(mock_data)
