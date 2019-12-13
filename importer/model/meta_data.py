import pandas as pd
import logging
from importer import utility
from functools import reduce

logger = logging.getLogger('Excel XML processor')
utility.setup_logger(logger)

# FIXME MOVE TO meta-excel
LOOKUP_TABLENAMES = [
    'tbl_feature_types',
    'tbl_locations',
    'tbl_sample_group_sampling_contexts',
    'tbl_sample_location_types',
    'tbl_feature_types',
    'tbl_data_type_groups',
    'tbl_data_types',
    'tbl_dataset_masters',
    'tbl_error_uncertainties',
    'tbl_age_types',
    'tbl_season_or_qualifier',
    'tbl_sample_description_types',
    'tbl_biblio',
    'tbl_contacts',
    'tbl_project_types',
    'tbl_project_stages',
    'tbl_isotope_standards',
    'tbl_isotope_types'
];

class MetaData:

    '''
    Logic related to meta-data read from Excel file
    '''
    def __init__(self):
        self.Tables = None
        self.Columns = None
        self.PrimaryKeys = None
        self.ForeignKeys = None
        self.ForeignKeyAliases = {
            'updated_dataset_id': 'dataset_id'
        }
        self._ForeignKey_Hash = None
        self._PrimaryKey_Hash = None
        self._Classname_Cache = None

    def load(self, filename):

        def recode_excel_sheet_name(row):
            value = row['excel_sheet']
            if pd.notnull(value) and len(value) > 0 and value != 'nan':
                logger.info("Using alias %s for %s", value, row['table_name'])
                return value
            return row['table_name']

        self.Tables = pd.read_excel(filename, 'Tables',
            dtype={
                'table_name': 'str',
                'java_class': 'str',
                'pk_name': 'str',
                'excel_sheet': 'str',
                'notes': 'str'
            })

        self.Columns = pd.read_excel(filename, 'Columns',
            dtype={
                'table_name': 'str',
                'column_name': 'str',
                # 'position': np.int32,
                'nullable': 'str',
                'type': 'str',
                # 'length': np.int32,
                # 'size': np.int32,
                'type2': 'str',
                'class': 'str'
            })  # .set_index(['table_name', 'column_name'])

        self.Tables['table_name_index'] = self.Tables['table_name']
        self.Tables = self.Tables.set_index('table_name_index')

        self.Tables['excel_sheet'] = self.Tables.apply(recode_excel_sheet_name, axis=1)

        self.PrimaryKeys = pd.merge(self.Tables, self.Columns, how='inner', left_on=['table_name', 'pk_name'], right_on=['table_name', 'column_name'])[['table_name', 'column_name', 'java_class']]
        self.PrimaryKeys.columns = ['table_name', 'column_name', 'class_name']

        self.ForeignKeys = pd.merge(self.Columns, self.PrimaryKeys, how='inner', left_on=['column_name', 'class'], right_on=['column_name', 'class_name'])[['table_name_x', 'table_name_y', 'column_name', 'class_name' ]]
        self.ForeignKeys = self.ForeignKeys[self.ForeignKeys.table_name_x != self.ForeignKeys.table_name_y]

        # self.CandidateForeignKeys = self.Columns[self.Columns.str.endsWith('_id')]

        self._ForeignKey_Hash = {
            x: True for x in list(self.ForeignKeys.table_name_x + '#' + self.ForeignKeys.column_name)
        }

        self._PrimaryKey_Hash = {
            x: True for x in self.Tables.table_name + '#' + self.Tables.pk_name
        }

        self._Classname_Cache = self.Tables.set_index('java_class')['table_name'].to_dict()

        return self

    @property
    def tablenames(self):
        return self.Tables["table_name"].tolist()

    def table_fields(self, table_name):
        return self.Columns[(self.Columns.table_name == table_name)]

    # def get_columns(self, table_name):
    #     return self.Columns[(self.Columns.table_name == table_name)].to_dict()

    def is_table(self, table_name):
        return table_name in self.Tables.table_name.values

    def get_table(self, table_name):
        return self.Tables.loc[table_name].to_dict()

    def get_tablename_by_classname(self, class_name):
        try:
            if '.' in class_name:
                class_name = class_name.split('.')[-1]
            # return self.Tables.loc[(self.Tables.java_class == class_name)]['table_name'].iloc[0]
            return self._Classname_Cache[class_name]
        except: # pylint: disable=W0702
            logger.warning('get_tablename_by_classname Unknown class: %s', class_name)
            return None

    def is_fk(self, table_name, column_name):
        if column_name in self.ForeignKeyAliases:
            return True
        return (table_name + '#' + column_name) in self._ForeignKey_Hash
        # return ((self.Tables.table_name != table_name) & (self.Tables.pk_name == column_name)).any()

    def is_pk(self, table_name, column_name):
        return (table_name + '#' + column_name) in self._PrimaryKey_Hash
        # return ((self.Tables.table_name == table_name) & (self.Tables.pk_name == column_name)).any()

    def get_pk_name(self, table_name):
        try:
            return self.PrimaryKeys.loc[(self.PrimaryKeys.table_name == table_name)]['column_name'].iloc[0]
        except: # pylint: disable=W0702
            return None

    def get_classname_by_tablename(self, table_name):
        return self.PrimaryKeys.loc[(self.PrimaryKeys.table_name == table_name)]['class_name'].iloc[0]

    def get_tablenames_referencing(self, table_name):
        return self.ForeignKeys.loc[(self.ForeignKeys.table_name_y == table_name)]['table_name_x'].tolist()

    def is_lookup_table(self, table_name):
        return table_name in LOOKUP_TABLENAMES

# FIXME: class CandidateKeySpecification():

