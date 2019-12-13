import sys
import os

sys.path = sys.path + ['.']

from importer.process import process

data_folder = './data'

cmd_opts_isotope = "--host {} --dbname {} --dbuser {} --input-folder {} --output-folder {} --data-filename {} --meta-filename {} --data-types {}".format(
    "seadserv.humlab.umu.se",
    "sead_staging_clearinghouse",
    "clearinghouse_worker",
    os.path.join(data_folder, "input"),
    os.path.join(data_folder, "output"),
    "isotope_data_latest_20191213.xlsm",
    "metadata_latest_20191104.xlsx",
    "isotope"
)

cmd_opts_ceramics = "--host {} --dbname {} --dbuser {} --input-folder {} --output-folder {} --data-filename {} --meta-filename {} --data-types {}".format(
    "seadserv.humlab.umu.se",
    "sead_staging",
    "clearinghouse_worker",
    os.path.join(data_folder, "input"),
    os.path.join(data_folder, "output"),
    "ceramics_data_latest_20191213.xlsx",
    "metadata_latest_20191104.xlsx",
    "ceramics"
)

process(cmd_opts_ceramics)

