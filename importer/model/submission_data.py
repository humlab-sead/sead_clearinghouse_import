import logging
import functools
import pandas as pd
import numpy as np
from importer import utility
from . exceptions import DataImportError

logger = logging.getLogger('Excel XML processor')
utility.setup_logger(logger)

class SubmissionData:

    '''
    Logic dealing with the submission (load etc)
    '''
    def __init__(self, metaData):
        self.MetaData = metaData
        self.DataTables = None
        self.DataTableIndex = None

    def load_sheet(self, reader, sheetname):
        df = None
        try:
            df = reader.parse(sheetname)
        except: # pylint: disable=bare-except
            pass
        logger.info('SHEET %s: %s', sheetname, 'READ' if df is not None else 'NOT FOUND')
        return df

    def load(self, source):

        reader = pd.ExcelFile(source) if isinstance(source, str) else source

        self.DataTables = {
            x['table_name']: self.load_sheet(reader, x['excel_sheet']) for i, x in self.MetaData.Tables.iterrows()
        }

        self.DataTableIndex = self.load_sheet(reader, 'data_table_index')

        if self.DataTableIndex is None:
            logger.exception('submission file has no data_table_index')

        reader.close()

        return self

    def store(self, filename):
        writer = pd.ExcelWriter(filename) # pylint: disable=abstract-class-instantiated
        for (table_name, df) in self.DataTables:
            df.to_excel(writer, table_name)  # , index=False)
        writer.save()
        return self

    def exists(self, table_name):
        return table_name in self.DataTables.keys() and self.DataTables[table_name] is not None

    def has_system_id(self, table_name):
        return self.exists(table_name) and 'system_id' in self.DataTables[table_name].columns

    @property
    def tablenames(self):
        return [ x for x in self.DataTables.keys() if self.exists(x) ]

    @property
    def data_tablenames(self):
        # return self.MetaData.tables_with_data()
        return self.DataTableIndex["table_name"].tolist()

    def get_referenced_keyset(self, table_name):
        pk_name = self.MetaData.get_pk_name(table_name)
        if pk_name is None:
            return []
        ref_tablenames = self.MetaData.get_tablenames_referencing(table_name)
        sets_of_keys = [
            set(self.DataTables[foreign_name][pk_name].loc[~np.isnan(self.DataTables[foreign_name][pk_name])].tolist())
            for foreign_name in ref_tablenames if not self.DataTables[foreign_name] is None
        ]
        return functools.reduce(utility.flatten_sets, sets_of_keys or [], [])