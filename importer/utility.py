import base64
import fnmatch
import functools
import importlib
import io
import os
import re
import sys
import zlib
from datetime import datetime
from os.path import abspath, basename, dirname, join, splitext
from typing import Any, Callable, Literal
from xml.dom import minidom

import pandas as pd
import yaml
from loguru import logger
from sqlalchemy import Engine, create_engine


def configure_logging(opts: dict[str, str]) -> None:

    if not opts:
        return

    if opts.get("handlers"):

        for handler in opts["handlers"]:

            if not handler.get("sink"):
                continue

            if handler["sink"] == "sys.stdout":
                handler["sink"] = sys.stdout

            elif isinstance(handler['sink'], str) and handler['sink'].endswith(".log"):
                handler["sink"] = join(
                    opts.get('folder', 'logs'), f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{handler['sink']}"
                )

        logger.configure(handlers=opts["handlers"])


def pascal_to_snake_case(s: str) -> str:
    """
    Converts a string from PascalCase to snake_case.
    """
    return re.sub(r'(?<!^)(?=[A-Z])', '_', s).lower()


def snake_to_pascal_case(s: str) -> str:
    return ''.join(part.capitalize() for part in s.split('_'))


def import_sub_modules(module_folder: str) -> Any:
    __all__ = []
    # current_dir: str = os.path.dirname(__file__)
    for filename in os.listdir(module_folder):
        if filename.endswith(".py") and filename != "__init__.py":
            module_name: str = filename[:-3]
            __all__.append(module_name)
            importlib.import_module(f".{module_name}", package=__name__)


def recursive_update(d1: dict, d2: dict) -> dict:
    """
    Recursively updates d1 with values from d2. If a value in d1 is a dictionary,
    and the corresponding value in d2 is also a dictionary, it recursively updates that dictionary.
    """
    for key, value in d2.items():
        if isinstance(value, dict) and key in d1 and isinstance(d1[key], dict):
            recursive_update(d1[key], value)
        else:
            d1[key] = value
    return d1


def recursive_filter_dict(
    D: dict[str, Any], filter_keys: set[str], filter_mode: Literal['keep', 'exclude'] = 'exclude'
) -> dict[str, Any]:
    """
    Recursively filters a dictionary to include only keys in the given set.

    Args:
        D (dict): The dictionary to filter.
        filter_keys (set): The set of keys to keep or exclude.
        filter_mode (str): mode of operation, either 'keep' or 'exclude'.

    Returns:
        dict: A new dictionary containing only the keys in K, with nested dictionaries also filtered.
    """
    if not isinstance(D, dict):
        return D

    return {
        key: (
            recursive_filter_dict(value, filter_keys=filter_keys, filter_mode=filter_mode)
            if isinstance(value, dict)
            else value
        )
        for key, value in D.items()
        if (key in filter_keys if filter_mode == 'keep' else key not in filter_keys)
    }


def dget(data: dict, *path: str | list[str], default: Any = None) -> Any:
    if path is None or not data:
        return default

    ps: list[str] = path if isinstance(path, (list, tuple)) else [path]

    d = None

    for p in ps:
        d = dotget(data, p)

        if d is not None:
            return d

    return d or default


def dotexists(data: dict, *paths: list[str]) -> bool:
    for path in paths:
        if dotget(data, path, default="@@") != "@@":
            return True
    return False


def dotexpand(path: str) -> list[str]:
    """Expands paths with ',' and ':'."""
    paths: list[str] = []
    for p in path.replace(' ', '').split(','):
        if not p:
            continue
        if ':' in p:
            paths.extend([p.replace(":", "."), p.replace(":", "_")])
        else:
            paths.append(p)
    return paths


def dotget(data: dict, path: str, default: Any = None) -> Any:
    """Gets element from dict. Path can be x.y.y or x_y_y or x:y:y.
    if path is x:y:y then element is search using borh x.y.y or x_y_y."""

    for key in dotexpand(path):
        d: dict = data
        for attr in key.split('.'):
            d: dict = d.get(attr) if isinstance(d, dict) else None
            if d is None:
                break
        if d is not None:
            return d
    return default


def dotset(data: dict, path: str, value: Any) -> dict:
    """Sets element in dict using dot notation x.y.z or x:y:z"""

    d: dict = data
    attrs: list[str] = path.replace(":", ".").split('.')
    for attr in attrs[:-1]:
        if not attr:
            continue
        d: dict = d.setdefault(attr, {})
    d[attrs[-1]] = value

    return data


def env2dict(prefix: str, data: dict[str, str] | None = None, lower_key: bool = True) -> dict[str, str]:
    """Loads environment variables starting with prefix into."""
    if data is None:
        data = {}
    if not prefix:
        return data
    for key, value in os.environ.items():
        if lower_key:
            key = key.lower()
        if key.startswith(prefix.lower()):
            dotset(data, key[len(prefix) + 1 :].replace('_', ':'), value)
    return data


def log_decorator(
    enter_message: str | None = 'Entering', exit_message: str | None = 'Exiting', level: int | str = "INFO"
):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):

            if not (enter_message or exit_message):
                return func(*args, **kwargs)

            if enter_message:
                logger.log(level, f'{enter_message} ({func.__name__})')
            result = func(*args, **kwargs)
            if exit_message:
                logger.log(level, f'{exit_message} ({func.__name__})')
            return result

        return wrapper

    return decorator


# def load_sql_from_file(identifier: str) -> str:
#     sql_path: str = join(dirname(abspath(__file__)), "sql", identifier + ".sql")
#     if not os.path.exists(sql_path):
#         return
#     with open(sql_path, "r") as file:
#         return file.read()


def load_json_from_file(identifier: str) -> str:
    sql_path: str = join(dirname(abspath(__file__)), "json", identifier + ".json")
    with open(sql_path, "r") as file:
        return file.read()


def upload_dataframe_to_postgres(df: pd.DataFrame, table_name: str, db_uri: str) -> None:
    """
    Uploads a pandas DataFrame to a PostgreSQL database.

    Parameters:
    df (pd.DataFrame): The DataFrame to upload.
    table_name (str): The name of the table to upload the DataFrame to.
    db_uri (str): The URI of the PostgreSQL database.
    """
    engine: Engine = create_engine(db_uri)

    df.to_sql(table_name, engine, schema="public", if_exists="fail", index=False)


def load_dataframe_from_postgres(sql: str, db_uri: str, index_col: str = None, dtype: Any = None) -> pd.DataFrame:
    """
    Loads a pandas DataFrame from a PostgreSQL database.

    Parameters:
    sql (str): The name of the table to load the DataFrame from.
    db_uri (str): The URI of the PostgreSQL database.
    """
    engine: Engine = create_engine(db_uri)
    sql = sql.replace("%", "%%")
    return pd.read_sql_query(sql, con=engine, index_col=index_col, dtype=dtype)


def load_sead_data(db_uri: str, sql: str | pd.DataFrame, index: list[str], sortby: list[str] = None) -> pd.DataFrame:
    """Returns a dataframe of tables from SEAD with attributes."""
    index = index if isinstance(index, list) else [index]
    sortby = sortby if isinstance(sortby, list) else [sortby] if sortby else None
    data: pd.DataFrame = (
        (sql if isinstance(sql, pd.DataFrame) else load_dataframe_from_postgres(sql, db_uri, index_col=None))
        .set_index(index, drop=False)
        .rename_axis([f'index_{x}' for x in index])
        .sort_values(by=sortby if sortby else index)
    )
    return data


def load_sead_columns(db_uri: str, ignore_columns: list[str] = None) -> pd.DataFrame:
    """Returns a dataframe of table columns from SEAD with attributes."""
    sql: str = "select * from clearing_house.clearinghouse_import_columns"
    data: pd.DataFrame = load_sead_data(db_uri, sql, ["table_name", "column_name"], ["table_name", "position"])
    if ignore_columns:
        columns_to_ignore: list[str] = [
            c for c in data['column_name'].unique() if any(fnmatch.fnmatch(c, pattern) for pattern in ignore_columns)
        ]
        data = data[~data['column_name'].isin(columns_to_ignore)]

    return data


def flatten(l) -> list:
    """
    Flattens a list of lists
    """
    return [item for sublist in l for item in sublist]


def flatten_sets(x, y) -> set:
    """
    Flattens a set of sets
    """
    return set(list(x) + list(y))


def camel_case_name(undescore_name: str) -> str:
    first, *rest = undescore_name.split("_")
    return first + "".join(word.capitalize() for word in rest)


def tidy_xml(path: str, suffix: str = "_tidy", remove_source: bool = True) -> str:
    try:
        doc = minidom.parse(path)
        tidy_doc = doc.toprettyxml(encoding="UTF-8", newl="")
        tidy_path: str = path[:-4] + "{}.xml".format(suffix)
        with io.open(tidy_path, "wb") as outstream:
            outstream.write(tidy_doc)
    except OSError as _:
        logger.error("fatal: Tidy XML failed. Is tidy installed? (sudo apt-get install tidy)")
        return path

    if remove_source:
        os.remove(path)

    return tidy_path


def compress_and_encode(path: str) -> None:
    compressed_data: bytes = zlib.compress(path.encode("utf8"))
    encoded: bytes = base64.b64encode(compressed_data)
    uue_filename: str = path + ".gz.uue"
    with io.open(uue_filename, "wb") as outstream:
        outstream.write(encoded)

    gz_filename: str = path + ".gz"
    with io.open(gz_filename, "wb") as outstream:
        outstream.write(compressed_data)


class Registry:
    items: dict = {}

    @classmethod
    def get(cls, key: str) -> Any | None:
        if key not in cls.items:
            raise ValueError(f"preprocessor {key} is not registered")
        return cls.items.get(key)

    @classmethod
    def register(cls, **args) -> Callable[..., Any]:
        def decorator(fn):
            if args.get("type") == "function":
                fn = fn()
            cls.items[args.get("key") or fn.__name__] = fn
            return fn

        return decorator

    @classmethod
    def is_registered(cls, key: str) -> bool:
        return key in cls.items


def strip_path_and_extension(filename: str | list[str]) -> str | list[str]:
    """Remove path and extension from filename(s). Return list."""
    if isinstance(filename, str):
        return splitext(basename(filename))[0]
    return [splitext(basename(x))[0] for x in filename]


def strip_extensions(filename: str | list[str]) -> str | list[str]:
    if isinstance(filename, str):
        return splitext(filename)[0]
    return [splitext(x)[0] for x in filename]


def replace_extension(filename: str, extension: str) -> str:
    if filename.endswith(extension):
        return filename
    base, _ = splitext(filename)
    return f"{base}{'' if extension.startswith('.') else '.'}{extension}"


def path_add_suffix(path: str, suffix: str, new_extension: str = None) -> str:
    name, extension = splitext(path)
    return f'{name}{suffix}{extension if new_extension is None else new_extension}'


def path_add_timestamp(path: str, fmt: str = "%Y%m%d%H%M") -> str:
    return path_add_suffix(path, f'_{datetime.now().strftime(fmt)}')


def path_add_date(path: str, fmt: str = "%Y%m%d") -> str:
    return path_add_suffix(path, f'_{datetime.now().strftime(fmt)}')


def ts_data_path(directory: str, filename: str) -> str:
    return join(directory, f'{datetime.now().strftime("%Y%m%d%H%M")}_{filename}')


def read_yaml(file: Any) -> dict:
    """Read yaml file. Return dict."""
    if isinstance(file, str) and any(file.endswith(x) for x in ('.yml', '.yaml')):
        with open(file, "r", encoding='utf-8') as fp:
            return yaml.load(fp, Loader=yaml.FullLoader)
    data: list[dict] = yaml.load(file, Loader=yaml.FullLoader)
    return {} if len(data) == 0 else data[0]


def write_yaml(data: dict, file: str) -> None:
    """Write yaml to file.."""
    with open(file, "w", encoding='utf-8') as fp:
        return yaml.dump(data=data, stream=fp)


def remove_keys_recursively(data: dict[str, Any], keys_to_remove: set[str]) -> None:
    """
    Recursively removes keys from a dictionary if they exist in keys_to_remove.

    Parameters:
    data (dict): The input dictionary.
    keys_to_remove (set): A set of keys to be removed.
    """
    if not isinstance(data, dict):
        return data

    if not keys_to_remove:
        return data

    keys: list[str] = list(data.keys())
    for key in keys:
        if key in keys_to_remove:
            del data[key]
        elif isinstance(data[key], dict):
            remove_keys_recursively(data[key], keys_to_remove)


def update_dict_from_yaml(yaml_file: str, data: dict, keep_keys: set[str]) -> dict:
    """Update dict `data` with values found in `yaml_file`."""
    if yaml_file is None:
        return data
    keep_keys = keep_keys or set()
    options: dict = read_yaml(yaml_file)
    data.update(options)
    return data


def create_db_uri(*, host: str, port: int | str, user: str, dbname: str) -> str:
    """
    Returns the database URI from the environment variables.
    """
    return f"postgresql://{user}@{host}:{port}/{dbname}"


def get_connection_uri(connection: Any) -> str:
    conn_info = connection.get_dsn_parameters()
    user: str = conn_info.get('user')
    host: str = conn_info.get('host')
    port: str = conn_info.get('port')
    dbname: str = conn_info.get('dbname')
    uri: str = f"postgresql://{user}@{host}:{port}/{dbname}"
    return uri
