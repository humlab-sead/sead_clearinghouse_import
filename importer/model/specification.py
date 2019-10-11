class DataTableSpecification:

    '''
    Specification class that tests validity of data
    '''
    def __init__(self):
        self.errors = []
        self.warnings = []
        self.ignore_columns = [ 'date_updated' ]

    def is_satisfied_by(self, data):

        self.errors = []
        self.warnings = []

        for table_name in data.data_tablenames:
            self.is_satisfied_by_table(data, table_name)

        return len(self.errors) == 0

    def is_satisfied_by_table(self, data, table_name):

        try:
            # Must exist as data table in metadata
            if table_name not in data.tablenames:
                self.errors.append("{0} not defined as data table".format(table_name))

            # Must have a system identit
            if not data.has_system_id(table_name):
                self.errors.append("{0} has no system id data column".format(table_name))

            if not data.MetaData.is_table(table_name):
                self.errors.append("{0} not found in meta data".format(table_name))

            if not data.exists(table_name):
                self.errors.append("{0} has NO DATA!".format(table_name))

            # Verify that all fields in MetaFields exists DataTable.columns
            meta_column_names = sorted(data.MetaData.table_fields(table_name)['column_name'].values.tolist())
            data_column_names = sorted(data.DataTables[table_name].columns.values.tolist()) \
                if data.exists(table_name) and data.MetaData.is_table(table_name) else []

            missing_column_names = list(set(meta_column_names) - set(data_column_names) - set(self.ignore_columns))
            extra_column_names = list(set(data_column_names) - set(meta_column_names) - set(self.ignore_columns) - set(['system_id']))

            if len(missing_column_names) > 0:
                self.errors.append("ERROR {0} has MISSING DATA columns: ".format(table_name) + (", ".join(missing_column_names)))

            if len(extra_column_names) > 0:
                self.warnings.append("WARNING {0} has EXTRA DATA columns: ".format(table_name) + (", ".join(extra_column_names)))

            data_table = data.DataTables[table_name]

            if data_table is not None:

                if 'system_id' not in data_table.columns:
                    self.errors.append('CRITICAL ERROR Table {} has no column named "system_id"'.format(table_name))

                pk_name = data.MetaData.get_pk_name(table_name)
                if pk_name not in data_table.columns:
                    self.errors.append('CRITICAL ERROR Table {} has no PK named "{}"'.format(table_name, pk_name))

                fields = data.MetaData.table_fields(table_name)
                type_compatibility_matrix = {
                    ('integer', 'float64'): True,
                    ('timestamp with time zone', 'float64'): False,
                    ('text', 'float64'): False,
                    ('character varying', 'float64'): False,
                    ('numeric', 'float64'): True,
                    ('timestamp without time zone', 'float64'): False,
                    ('boolean', 'float64'): False,
                    ('date', 'float64'): False,
                    ('smallint', 'float64'): True,
                    ('integer', 'object'): False,
                    ('timestamp with time zone', 'object'): True,
                    ('text', 'object'): True,
                    ('character varying', 'object'): True,
                    ('numeric', 'object'): False,
                    ('timestamp without time zone', 'object'): True,
                    ('boolean', 'object'): False,
                    ('date', 'object'): True,
                    ('smallint', 'object'): False,
                    ('integer', 'int64'): True,
                    ('timestamp with time zone', 'int64'): False,
                    ('text', 'int64'): False,
                    ('character varying', 'int64'): False,
                    ('numeric', 'int64'): True,
                    ('timestamp without time zone', 'int64'): False,
                    ('boolean', 'int64'): False,
                    ('date', 'int64'): False,
                    ('smallint', 'int64'): True,
                    ('timestamp with time zone', 'datetime64[ns]'): True,
                    ('date', 'datetime64[ns]'): True,
                    #  ('character varying', 'datetime64[ns]'): True
                }

                for _, item in fields.loc[(~fields.column_name.isin(self.ignore_columns))].iterrows():

                    column = item.to_dict()

                    column_name = column['column_name']

                    if column_name in data_table.columns:

                        data_column_type = data_table.dtypes[column_name].name

                        if not type_compatibility_matrix.get((column['type'], data_column_type), False):
                            self.warnings.append("WARNING Type clash: {}.{} {}<=>{}".format(table_name, column_name, column['type'], data_column_type))

                        is_fk = data.MetaData.is_fk(table_name, column_name)
                        is_pk = data.MetaData.is_pk(table_name, column_name)

                        if column_name[-3:] == '_id' and not (is_fk or is_pk):
                            self.warnings.append('WARNING! Table %s, column %s ends with _id but NOT marked as PK/FK', table_name, column_name)

        except Exception as e:
            self.errors.append('Error occurred when validating {}: {}'.format(table_name, str(e)))

