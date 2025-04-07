import os
import sys
from typing import Any

import click
import dotenv
from loguru import logger

from importer.configuration.inject import ConfigStore, ConfigValue
from importer.metadata import Metadata
from importer.process import ImportService, Options
from importer.repository import SubmissionRepository
from importer.scripts.utility import update_arguments_from_options_file
from importer.submission import Submission
from importer.utility import configure_logging, strip_path_and_extension

dotenv.load_dotenv(dotenv.find_dotenv())

# pylint: disable=no-value-for-parameter,unused-argument,too-many-positional-arguments


@click.command()
@click.argument('key', type=str)
@click.option("--clear-header", type=str, help="Output folder", required=False)
@click.option("--clear-exploded", type=str, help="Remove (exploded) data in CH entity tables", required=False)
@click.option("--host", "-h", "host", type=str, help="Target database server")
@click.option("--database", "-d", "dbname", type=str, help="Database name")
@click.option("--user", "-u", "user", type=str, help="Database user")
@click.option("--port", "-p", "port", type=int, default=5432, help="Server port number.")
def remove_submission(
    key: str,
    clear_header: bool,
    clear_exploded: bool,
    host: str,
    dbname: str,
    user: str,
    port: str,
) -> None:
    """
    Removes a SEAD data submission from the SEAD ClearingHouse database.
    The `key` argument can be either a submission ID or submission name (i.e. CR name).
    """
    opts = dict(host=host, dbname=dbname, user=user, port=port)
    repository: SubmissionRepository = SubmissionRepository(opts, uploader=None)

    submission_id: int = int(key) if key.isdigit() else repository.get_id_by_name(key)

    if submission_id is None:
        logger.error(f"Submission {key} not found")
        sys.exit(1)

    repository.remove(submission_id, clear_header=clear_header, clear_exploded=clear_exploded)


if __name__ == "__main__":
    remove_submission()

    # print("WARNING: running using CliRunner (options are ignored)")

    # from click.testing import CliRunner

    # runner = CliRunner()
    # runner.invoke(
    #     remove_submission,
    #     [
    #         "test",
    #         False,
    #         False,
    #     ],
    # )
