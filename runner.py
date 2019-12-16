import argparse
import sys
import copy

sys.path = sys.path + ['.']

from importer.process import AppService

DEFAULTS_OPTS = argparse.Namespace(
    data_filename=None,
    data_types=None,
    meta_filename="metadata_latest_20191104.xlsx",
    dbhost='seadserv.humlab.umu.se',
    dbname='sead_staging',
    dbuser='clearinghouse_worker',
    input_folder='./data/input',
    output_folder='./data/output',
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

#import_file(data_filename='ceramics_data_latest_20191216.xlsx', data_types="Ceramics")
#import_file(data_filename='dendro_build_data_latest_20191213.xlsm', data_types="Dendro building")
#import_file(data_filename='dendro_ark_data_latest_20191213.xlsm',   data_types="Dendro archeology")
import_file(data_filename='isotope_data_latest_20191213.xlsm', data_types="Isotope", xml_filename="./data/output/isotope_data_latest_20191213_20191216-073242_tidy.xml")
