import base64
import functools
import io
import logging
import os
import zlib
from os.path import abspath, dirname, join
from typing import Any, Callable
from xml.dom import minidom

import dotenv
import pandas as pd
from loguru import logger
from sqlalchemy import Engine, create_engine


def log_decorator(enter_message='Entering', exit_message='Exiting', level=logging.INFO):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            logger.log(level, f'{enter_message}: {func.__name__}')
            result = func(*args, **kwargs)
            logger.log(level, f'{exit_message}: {func.__name__}')
            return result

        return wrapper

    return decorator


def load_sql_from_file(identifier: str) -> str:
    sql_path: str = join(dirname(abspath(__file__)), "sql", identifier + ".sql")
    with open(sql_path, "r") as file:
        return file.read()


def load_json_from_file(identifier: str) -> str:
    sql_path: str = join(dirname(abspath(__file__)), "json", identifier + ".json")
    with open(sql_path, "r") as file:
        return file.read()


def dburi_from_env() -> str:
    """
    Returns the database URI from the environment variables.
    """
    dotenv.load_dotenv(".env")
    return f"postgresql://{os.environ['DBUSER']}@{os.environ['DBHOST']}:5432/{os.environ['DBNAME']}"


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


def load_sead_data(db_uri: str, sql_id: str | pd.DataFrame, index: list[str], sortby: list[str] = None) -> pd.DataFrame:
    """Returns a dataframe of tables from SEAD with attributes."""
    index = index if isinstance(index, list) else [index]
    sortby = sortby if isinstance(sortby, list) else [sortby] if sortby else None
    data: pd.DataFrame = (
        (
            sql_id
            if isinstance(sql_id, pd.DataFrame)
            else load_dataframe_from_postgres(load_sql_from_file(sql_id), db_uri, index_col=None)
        )
        .set_index(index, drop=False)
        .rename_axis([f'index_{x}' for x in index])
        .sort_values(by=sortby if sortby else index)
    )
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


def tidy_xml(path: str, suffix: str = "_tidy", remove_source: bool = True) -> str:
    try:
        doc = minidom.parse(path)
        tidy_doc = doc.toprettyxml(encoding="UTF-8")
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
