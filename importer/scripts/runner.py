import argparse
import sys
import os
import copy

jj = os.path.join

sys.path = sys.path + ['.']

from importer.process import AppService

# ROOT_FOLDER = r"C:\Users\roma0050\Google Drive (roma0050@gapps.umu.se)\Project\Public\VISEAD (Humlab)\SEAD Ceramics & Dendro\input"
ROOT_FOLDER = r"./data"

DEFAULTS_OPTS = argparse.Namespace(
    data_filename=None,
    data_types=None,
    meta_filename="metadata_latest_20191104.xlsx",
    dbhost='seadserv.humlab.umu.se',
    dbname='sead_staging',
    dbuser='clearinghouse_worker',
    input_folder=jj(ROOT_FOLDER, 'input'),
    output_folder=jj(ROOT_FOLDER, 'output'),
    port=5432,
    skip=False,
    submission_id=None,
    table_names=None,
    xml_filename=None
)

def import_file(**kwargs):

    opts = copy.copy(DEFAULTS_OPTS)
    opts.__dict__.update(kwargs)

    AppService(opts).process()

import_file(data_filename='c14_import_20200224.xlsm', data_types="Ceramics")

# import_file(data_filename='dendro_build_data_latest_20191213.xlsm', data_types="Dendro building", xml_filename="./data/output/dendro_build_data_latest_20191213_20191217-151636_tidy.xml")
# import_file(data_filename='dendro_ark_data_latest_20191213.xlsm',  data_types="Dendro archeology", xml_filename="./data/output/dendro_ark_data_latest_20191213_20191217-152152_tidy.xml")
# import_file(data_filename='isotope_data_latest_20191218.xlsm', data_types="Isotope", xml_filename="./data/output/isotope_data_latest_20191218_20191218-134724_tidy.xml")
