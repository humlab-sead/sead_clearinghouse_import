import sys
import os

sys.path = sys.path + ['.']

from importer.process import main

data_folder = './data' #'"C:\\Users\\roma0050\\Google Drive (roma0050@gapps.umu.se)\\Project\\Public\\VISEAD (Humlab)\\SEAD Ceramics & Dendro\\Isotope import"'

cmd_opts_isotope = "--host {} --dbname {} --dbuser {} --input-folder {} --output-folder {} --data-filename {} --meta-filename {} --data-types {}".format(
    "seadserv.humlab.umu.se",
    "sead_staging_clearinghouse",
    "clearinghouse_worker",
    os.path.join(data_folder, "input"),
    os.path.join(data_folder, "output"),
    "isotope_data_latest_20191017.xlsm",
    "metadata_latest_20191017.xlsx",
    "isotope"
)

cmd_opts_ceramics = "--host {} --dbname {} --dbuser {} --input-folder {} --output-folder {} --data-filename {} --meta-filename {} --data-types {}".format(
    "seadserv.humlab.umu.se",
    "sead_staging_clearinghouse",
    "clearinghouse_worker",
    os.path.join(data_folder, "input"),
    os.path.join(data_folder, "output"),
    "ceramics_data_latest_20191003.xlsm",
    "ceramics_metadata_latest_20180701.xlsx",
    "ceramics"
)


main(cmd_opts_ceramics)

#python runner.py --host seadserv.humlab.umu.se --dbname sead_staging_clearinghouse --dbuser clearinghouse_worker --input-folder ./data/input --output-folder ./data/output/ --data-filename isotop_data_latest.xlsm --meta-filename metadata_latest.xlsx --data-types isotope
