import os
from datetime import datetime

import click
import dotenv
from loguru import logger

from importer.metadata import Metadata
from importer.process import ImportService, Options
from importer.submission import SubmissionData, load_excel

dotenv.load_dotenv(dotenv.find_dotenv())

# pylint: disable=no-value-for-parameter,unused-argument


@click.command()
@click.argument("filename")
@click.option("--data-types", "-t", type=str, help="Types of data (short description)", required=False)
@click.option("--output-folder", type=str, envvar="OUTPUT_FOLDER", help="Output folder")
@click.option("--host", "-h", "dbhost", type=str, envvar="DBHOST", help="Target database server")
@click.option("--database", "-d", "dbname", type=str, envvar="DBNAME", help="Database name")
@click.option("--user", "-u", "dbuser", type=str, envvar="DBUSER", help="Database user")
@click.option("--port", type=int, default=5432, help="Server port number.")
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
@click.option("--transfer-format", type=str, default='xml', help="Explode XML into public tables.")
def import_file(
    filename: str,
    data_types: str,
    dbhost: str,
    dbname: str,
    dbuser: str,
    output_folder: str,
    port: str,
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
) -> None:
    """
    Imports a new SEAD data submission to the SEAD ClearingHouse database. The source data is either
    an Excel file or an XML file that has previously been generated with this program.

    The content of the Excel file is processed and stored in an XML file that conforms to the
    clearinghouse data import schema.

    The Excel file must satisfy the following requirements:
      - The file must be in the Excel 2007+ format (xlsx)
      - The file must contain a sheet named `data_table_index' listing all tables in the submission having new or changed data.
      - The file must contain a sheet named as in SEADe' for each table in the submission.

    """
    logger.add(f"{log_folder}/logs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    opts: Options = Options(**locals())
    return workflow(opts)


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
        opts.submission_id: int = int(opts.filename)
        opts.filename: str = None

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

    submission: SubmissionData | str = (
        opts.submission_id if opts.use_existing_submission else opts.xml_filename if isinstance(opts.xml_filename, str) else load_excel(metadata=metadata, source=opts.filename)
    )
    ImportService(metadata=metadata, opts=opts).process(submission=submission)


if __name__ == "__main__":
    import_file()

    # import_file(data_filename='dendro_build_data_latest_20191213.xlsm', data_types="Dendro building", xml_filename="./data/output/dendro_build_data_latest_20191213_20191217-151636_tidy.xml")
    # import_file(data_filename='dendro_ark_data_latest_20191213.xlsm',  data_types="Dendro archeology", xml_filename="./data/output/dendro_ark_data_latest_20191213_20191217-152152_tidy.xml")
    # import_file(data_filename='isotope_data_latest_20191218.xlsm', data_types="Isotope", xml_filename="./data/output/isotope_data_latest_20191218_20191218-134724_tidy.xml")
