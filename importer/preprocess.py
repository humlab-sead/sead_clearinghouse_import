import logging
from . import model

import numpy as np

logger = logging.getLogger('Excel XML processor')

def update_system_id(submission):

    for table_name in submission.data_tablenames:
        try:
            data_table = submission.DataTables[table_name]
            table_definition = submission.MetaData.get_table(table_name)

            pk_name = table_definition['pk_name']

            if pk_name == 'ceramics_id':
                pk_name = 'ceramic_id'

            if data_table is None or pk_name not in data_table.columns:
                continue

            if 'system_id' not in data_table.columns:
                raise model.DataImportError('CRITICAL ERROR Table {} has no column named "system_id"'.format(table_name))

            # FIXME: Update system_id to public_id? Thids can't be right???
            # TODO: Add unique test on system_id??
            data_table.loc[np.isnan(data_table.system_id), 'system_id'] = data_table.loc[np.isnan(data_table.system_id), pk_name]

        except model.DataImportError as _:
            logger.error("ERROR {} when updating system_id ".format(table_name))
            logger.exception('update_system_id')
            continue

    return submission

# def cast_table(submission, table_name):
#     data_table = submission.DataTables[table_name]
#     fields = submission.MetaData.table_fields(table_name)
#     for _, item in fields.iterrows():
#         column = item.to_dict()
#         if column['column_name'] in data_table.columns:
#             if column['type'] in ['integer']:
#                 submission.DataTables[table_name].astype(np.int64)

