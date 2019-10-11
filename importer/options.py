# -*- coding: utf-8 -*-
import os
import argparse

def setup_parser():

    parser = argparse.ArgumentParser(add_help=True)

    parser.add_argument('--host', help='target database server', dest="dbhost", required=True, default="seadserv.humlab.umu.se")
    parser.add_argument('--port', help='server port number', dest="port", type=int, default=5432)
    parser.add_argument('--dbname', help='target database', action="store", dest="dbname", required=True)
    parser.add_argument('--dbuser', help='target database username', action="store", dest="dbuser", default=os.environ.get('SEAD_CH_USER', 'clearinghouse_worker'))
    parser.add_argument('--input-folder', help='source folder where input files are stored', action="store", dest="input_folder", required=True, default='./input')
    parser.add_argument('--output-folder', help='target folder where result is stored', required=True, dest="output_folder", default='./output')
    parser.add_argument('--data-filename', help='name of file that contains data', required=True, dest="data_filename")
    parser.add_argument('--meta-filename', help='name of file that contains meta-data', required=False, dest="meta_filename", default='metadata_latest.xlsx')
    parser.add_argument('--xml-filename', help='name of existing XML to use', required=False, dest="xml_filename", default=None)
    parser.add_argument('--id', help='overwrite (replace) existing submission id', dest="submission_id", required=False)
    parser.add_argument('--table-names', help='load specific tables only', required=False)
    parser.add_argument('--data-types', help='types of data (short description)', action="store", dest="data_types", required=True)
    parser.add_argument('--skip', help='skip (do nothing)', action="store_true", default=False)

    return parser

def parse_args(args=None):
    parser = setup_parser()
    if args is not None:
        argv = args.split() if isinstance(args, str) else argv
        opts = parser.parse_args(argv)
    else:
        opts = parser.parse_args()
    return opts
