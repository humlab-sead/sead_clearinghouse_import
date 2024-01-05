import os
from datetime import datetime

import click
import dotenv
from loguru import logger

from importer.model.metadata import Metadata
from importer.model.submission import SubmissionData, load_excel
from importer.process import ImportService, Options

dotenv.load_dotenv(dotenv.find_dotenv())

# pylint: disable=no-value-for-parameter,unused-argument


@click.command()
@click.argument("data_filename")
@click.argument("meta_filename")
@click.option("--data-types", "-t", help="Types of data (short description)", required=True)
@click.option("--host", "-h", "dbhost", envvar="DBHOST", help="Target database server")
@click.option("--database", "-d", "dbname", envvar="DBNAME", help="Database name")
@click.option("--user", "-u", "dbuser", envvar="DBUSER", help="Database user")
@click.option("--input-folder", envvar="INPUT_FOLDER", help="Input folder")
@click.option("--output-folder", envvar="OUTPUT_FOLDER", help="Output folder")
@click.option("--port", default=5432, help="Server port number.")
@click.option("--skip", default=False, help="Skip the import (do nothing)")
@click.option("--id", "submission_id", default=None, help="Replace existing submission.")
@click.option("--table-names", default=None, help="Only load specified tables.")
@click.option("--xml-filename", default=None, help="Name of existing XML file to use.")
@click.option("--check-only", type=bool, default=False, help="Only check if file seems OK.")
def import_file(
    data_filename: str,
    meta_filename: str,
    data_types: str,
    dbhost: str,
    dbname: str,
    dbuser: str,
    input_folder: str,
    output_folder: str,
    port: str,
    skip: str,
    submission_id: str,
    table_names: str,
    xml_filename: str,
    check_only: bool,
) -> None:
    logger.add(f"logs/logs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

    opts: Options = Options(locals())
    return workflow(opts)


def workflow(opts: Options) -> None:
    metadata: Metadata = Metadata(opts.db_uri())

    if isinstance(opts.xml_filename, str):
        if not os.path.isfile(opts.xml_filename):
            logger.error(f" ---> file '{opts.xml_filename}' does not exist")
            return

        if opts.check_only:
            logger.error("The --check-only option is not supported when using an existing XML file")
            return

    submission: SubmissionData | str = (
        opts.xml_filename if isinstance(opts.xml_filename, str) else load_excel(metadata=metadata, source=opts.source())
    )
    ImportService(metadata=metadata, opts=opts).process(submission=submission)


if __name__ == "__main__":
    import_file()

    # import_file(data_filename='dendro_build_data_latest_20191213.xlsm', data_types="Dendro building", xml_filename="./data/output/dendro_build_data_latest_20191213_20191217-151636_tidy.xml")
    # import_file(data_filename='dendro_ark_data_latest_20191213.xlsm',  data_types="Dendro archeology", xml_filename="./data/output/dendro_ark_data_latest_20191213_20191217-152152_tidy.xml")
    # import_file(data_filename='isotope_data_latest_20191218.xlsm', data_types="Isotope", xml_filename="./data/output/isotope_data_latest_20191218_20191218-134724_tidy.xml")
