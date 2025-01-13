import os
import sys
from datetime import datetime
from typing import Any

import click
import dotenv
from loguru import logger

from importer.configuration.inject import ConfigStore, ConfigValue
from importer.metadata import Metadata
from importer.process import ImportService, Options
from importer.scripts.utility import update_arguments_from_options_file
from importer.submission import Submission
from importer.utility import configure_logging, dotset, strip_path_and_extension

dotenv.load_dotenv(dotenv.find_dotenv())

# pylint: disable=no-value-for-parameter,unused-argument,too-many-positional-arguments


@click.command()
@click.argument('config_filename')
@click.argument("filename")
@click.option(
    '--options-filename', type=str, default=None, help='Name of options file to use (alternative to CLI options).'
)
@click.option("--data-types", "-t", type=str, help="Types of data (short description)", required=False)
@click.option("--output-folder", type=str, help="Output folder", required=True)
@click.option("--host", "-h", "host", type=str, help="Target database server")
@click.option("--database", "-d", "dbname", type=str, help="Database name")
@click.option("--user", "-u", "user", type=str, help="Database user")
@click.option("--port", "-p", "port", type=int, default=5432, help="Server port number.")
@click.option("--skip", default=False, is_flag=True, help="Skip the import (do nothing)")
@click.option("--id", "submission_id", type=int, default=None, help="Replace existing submission.")
@click.option("--table-names", type=str, default=None, help="Only load specified tables.")
@click.option("--xml-filename", type=str, default=None, help="Name of existing XML file to use.")
@click.option("--log-folder", type=str, default="./logs", help="Name of existing XML file to use.")
@click.option("--check-only", type=bool, is_flag=True, default=False, help="Only check if file seems OK.")
@click.option("--register/--no-register", type=bool, is_flag=True, default=False, help="Register file in the database.")
@click.option("--explode/--no-explode", type=bool, is_flag=True, default=False, help="Explode XML into public tables.")
@click.option(
    "--tidy-xml/--no-tidy-xml", type=bool, is_flag=True, default=False, help="Run XML formatting tool on document."
)
@click.option(
    "--timestamp/--no-timestamp", type=bool, is_flag=True, default=True, help="Add timestamp to target XML filename."
)
@click.option("--transfer-format", type=str, default='xml', help="Specify format to use in upload (XML or CSV).")
@click.pass_context
def import_file(
    ctx,
    config_filename: str,
    filename: str,
    data_types: str,
    host: str,
    dbname: str,
    user: str,
    port: str,
    output_folder: str,
    skip: str,
    submission_id: str,
    table_names: str,
    xml_filename: str,
    check_only: bool,
    register: bool,
    explode: bool,
    log_folder: str,
    timestamp: bool,
    tidy_xml: bool,
    transfer_format: str,
    options_filename: str = None,
) -> None:
    """
    Imports a new SEAD data submission to the SEAD ClearingHouse database. The source data is either
    an Excel file or an XML file that has previously been generated with this program.

    The content of the Excel file is processed and stored in an XML file that conforms to the
    clearinghouse data import schema.

    The Excel file must satisfy the following requirements:
      - The file must be in the Excel 2007+ format (xlsx)
      - The file must contain a sheet named as in SEAD' for each table in the submission.

    """

    setup_configuration(ctx, dict(locals()))

    return workflow(opts=Options(**ConfigValue('options').resolve()))


def setup_configuration(ctx, opts: dict[str, Any]) -> None:

    specified_keys: set[str]=_get_specified_cli_opts(ctx)
    config_filename: str = opts.pop('config_filename')
    log_folder: str = opts.pop('log_folder')

    if not os.path.isfile(config_filename):
        logger.error(f" ---> file '{config_filename}' does not exist")
        sys.exit(1)

    opts = update_arguments_from_options_file(
        arguments=opts, filename_key='options_filename', suffix=strip_path_and_extension(opts.get("filename")), ctx=ctx
    )
    opts['database'] = {k: opts.pop(k) for k in ['host', 'dbname', 'user', 'port']}

    ConfigStore.configure_context(source=config_filename, env_filename='.env', env_prefix="SEAD_IMPORT")

    ConfigStore().consolidate(opts, context="default", section="options", ignore_keys=specified_keys)

    configure_logging(ConfigValue('logging').resolve() | ({} if not log_folder else {"folder": log_folder}))


def _get_specified_cli_opts(ctx) -> set[str]:
    return {key for key in ctx.params if ctx.get_parameter_source(key) == click.core.ParameterSource.COMMANDLINE}


def workflow(opts: Options) -> None:
    """
    Executes the workflow based on the given options.

    Parameters:
        opts (Options): An instance of the Options class containing the workflow configuration.

    Returns:
        None: This function does not return any value.
    """
    metadata: Metadata = Metadata(opts.db_uri())

    if opts.filename.isnumeric():
        opts.submission_id = int(opts.filename)
        opts.filename = None

    if not opts.use_existing_submission:

        if opts.filename.endswith(".xml"):
            opts.xml_filename = opts.filename
            opts.filename = None

        if isinstance(opts.xml_filename, str):
            if not os.path.isfile(opts.xml_filename):
                logger.error(f" ---> file '{opts.xml_filename}' does not exist")
                return

            if opts.check_only:
                logger.error("The --check-only option is not supported when using an existing XML file")
                return

    submission: Submission | str = (
        opts.submission_id
        if opts.use_existing_submission
        else (
            opts.xml_filename
            if isinstance(opts.xml_filename, str)
            else Submission.load(metadata=metadata, source=opts.filename)
        )
    )
    ImportService(metadata=metadata, opts=opts).process(submission=submission)


if __name__ == "__main__":
    import_file()

    from click.testing import CliRunner

    # print("warning: running using CliRunner (options are ignored)")
    # runner = CliRunner()
    # runner.invoke(
    #     import_file,
    #     [
    #         "./config.yml",
    #         "./data/input/SEAD_aDNA_data_20241114_RM.xlsx",
    #         "--no-timestamp",
    #         "--register",
    #         "--explode",
    #         "--database", "APA",
    #         "--data-types",
    #         "adna",
    #         "--transfer-format",
    #         "csv",
    #         "--output-folder",
    #         "./data/output/",
    #     ],
    # )
