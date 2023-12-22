
from importer import options
import logging

def test_parse_by_argsparse_parse_args():
    cmd_opts = "--host seadserv.humlab.umu.se --dbname sead_staging_clearinghouse --dbuser clearinghouse_worker --input-folder ./data/input --output-folder ./data/output/ --data-filename isotop_data_latest.xlsm --meta-filename metadata_latest.xlsx --data-types isotope"
    argv = cmd_opts.split()
    parser = options.setup_parser()
    opts = parser.parse_args(argv)
    logging.error(opts)
    assert opts is not None
    assert opts.dbhost == "seadserv.humlab.umu.se"
    assert opts.dbname == "sead_staging_clearinghouse"

def test_parse_by_options_parse_args():
    cmd_opts = "--host seadserv.humlab.umu.se --dbname sead_staging_clearinghouse --dbuser clearinghouse_worker --input-folder ./data/input --output-folder ./data/output/ --data-filename isotop_data_latest.xlsm --meta-filename metadata_latest.xlsx --data-types isotope"
    opts = options.parse_args(cmd_opts)
    assert opts.dbhost == "seadserv.humlab.umu.se"
    assert opts.dbname == "sead_staging_clearinghouse"
