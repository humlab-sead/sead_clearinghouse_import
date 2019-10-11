import logging
import functools
import pandas as pd
import numpy as np
from importer import utility
from . exceptions import DataImportError

logger = logging.getLogger('Excel XML processor')
utility.setup_logger(logger)

class ValueData:

    '''
    Logic dealing with the data (load etc)
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
        for j, y in self.MetaData.Tables.iterrows():
            print("{} - {}".format(y['table_name'], y['excel_sheet']))

        self.DataTables = {
            x['table_name']: self.load_sheet(reader, x['excel_sheet']) for i, x in self.MetaData.Tables.iterrows()
        }

        self.DataTableIndex = self.load_sheet(reader, 'data_table_index')

        if self.DataTableIndex is None:
            logger.exception('Data file has no data_table_index')

        reader.close()
        self.update_system_id()
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

    # def cast_table(self, table_name):
    #     data_table = self.ValueData.Tables[table_name]
    #     fields = self.MetaData.table_fields(table_name)
    #     for _, item in fields.iterrows():
    #         column = item.to_dict()
    #         if column['column_name'] in data_table.columns:
    #             if column['type'] in ['integer']:
    #                 self.ValueData.Tables[table_name].astype(np.int64)

    def update_system_id(self):

        for table_name in self.data_tablenames:
            try:
                data_table = self.DataTables[table_name]
                table_definition = self.MetaData.get_table(table_name)

                pk_name = table_definition['pk_name']

                if pk_name == 'ceramics_id':
                    pk_name = 'ceramic_id'

                if data_table is None or pk_name not in data_table.columns:
                    continue

                if 'system_id' not in data_table.columns:
                    raise DataImportError('CRITICAL ERROR Table {} has no column named "system_id"'.format(table_name))

                data_table.loc[np.isnan(data_table.system_id), 'system_id'] = data_table.loc[np.isnan(data_table.system_id), pk_name]
                # Change 20180628: Set system_id as index for fast
                # data_table.set_index('system_id', drop=False, inplace=True)
            except DataImportError as _:
                logger.error("ERROR {} when updating system_id ".format(table_name))
                logger.exception('update_system_id')
                continue
        return self

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