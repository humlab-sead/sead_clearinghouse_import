import click
import pandas as pd
from loguru import logger
from sqlalchemy.engine import URL, create_engine

# pylint: disable=no-value-for-parameter,unused-argument,too-many-positional-arguments


@click.command()
@click.argument("filename")
@click.argument("tablename")
@click.option("--delimiter", type=str, default=",", help="Character used to separate values in the input file.")
@click.option(
    "--quoting",
    type=int,
    default=0,
    help="Quoting style used in the input file (0=QUOTE_MINIMAL, 1=QUOTE_ALL, 2=QUOTE_NONNUMERIC, 3=QUOTE_NONE).",
)
@click.option("--database", "-d", "dbname", type=str, default=None, help="Target database name.")
@click.option("--host", "-h", "host", type=str, help="Target database server")
@click.option("--user", "-u", "user", type=str, help="Database user")
@click.option("--port", "-p", "port", type=str, default="5433", help="Server port number.")
@click.option(
    "--overwrite/--no-overwrite", type=bool, is_flag=True, default=False, help="Overwrite existing data if exists."
)
@click.pass_context
def import_csv(
    ctx,
    filename: str,
    tablename: str,
    delimiter: str,
    quoting: int,
    host: str,
    dbname: str,
    user: str,
    port: str,
    overwrite: bool,
) -> None:
    """
    Imports a CSV file into a table in the database using Pandas.
    """

    url: URL = URL.create(
        drivername="postgresql",
        username=user,
        host=host,
        port=port,
        database=dbname,
    )

    logger.info(f"Connecting to database '{url}'")

    data: pd.DataFrame = pd.read_csv(filename, sep=delimiter, quoting=quoting)  # type: ignore
    data.to_sql(name=tablename, con=create_engine(url), if_exists="replace" if overwrite else "fail", index=False)

    logger.info(f"Data imported successfully into table '{tablename}' in database '{dbname}'")


if __name__ == "__main__":
    import_csv()
