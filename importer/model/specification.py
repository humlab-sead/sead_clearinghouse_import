import numpy as np

TYPE_COMPATIBILITY_MATRIX = {
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

NUMERIC_TYPES = [ 'numeric', 'integer', 'smallint' ]

class DataTableSpecification:

    '''
    Specification class that tests validity of submission
    '''
    def __init__(self):
        self.errors = []
        self.warnings = []
        self.ignore_columns = [ 'date_updated' ]

    def _get_table_metadata_fields(self, submission, table_name):
        fields = submission.MetaData.table_fields(table_name)
        fields = fields.loc[(~fields.column_name.isin(self.ignore_columns))].iterrows()
        return fields

    def is_satisfied_by(self, submission):

        self.errors = []
        self.warnings = []

        for table_name in submission.data_tablenames:
            self.is_satisfied_by_table(submission, table_name)

        return len(self.errors) == 0

    def is_satisfied_by_table(self, submission, table_name):

        try:
            # Must exist as data table in metadata
            self.is_satisfied_by_table_must_exist_policy(submission, table_name)

            if not submission.exists(table_name):
                return

            data_table = submission.DataTables[table_name]

            self.is_satisfied_by_system_id_policy(submission, table_name, data_table)
            self.is_satisfied_by_no_missing_columns_policy(submission, table_name, data_table)
            self.is_satisfied_by_has_pk_policy(submission, table_name, data_table)
            self.is_satisfied_by_lookup_data_policy(submission, table_name)

            for _, field in self._get_table_metadata_fields(submission, table_name):

                column = field.to_dict()

                self.is_satisfied_by_type_match_policy(data_table, table_name, column)
                self.is_satisfied_by_is_numeric_policy(data_table, table_name, column)
                self.is_satisfied_by_id_is_fk_convention(submission, table_name, column)

        except Exception as e:
            self.errors.append('CRITICAL ERROR occurred when validating {}: {}'.format(table_name, str(e)))
            raise

    def is_satisfied_by_table_must_exist_policy(self, submission, table_name):

        if submission.exists(table_name) and table_name not in submission.tablenames:
            # Check if it has an alias
            meta = submission.MetaData.get_table(table_name)
            alias_name = meta['excel_sheet'] or 'no_alias'
            if alias_name not in submission.tablenames:
                """ Not in submission table index sheet """
                self.errors.append("CRITICAL ERROR Table {0} not defined as submission table".format(table_name))

        if not submission.exists(table_name):
            """ No data sheet """
            self.errors.append("{0} has NO DATA!".format(table_name))

    def is_satisfied_by_type_match_policy(self, data_table, table_name, column):

        column_name = column['column_name']

        if column_name not in data_table.columns:
            return

        if len(data_table) == 0:
            """ Cannot determine type if table is empty """
            return

        data_column_type = data_table.dtypes[column_name].name
        if not TYPE_COMPATIBILITY_MATRIX.get((column['type'], data_column_type), False):
            self.warnings.append("WARNING Type clash: {}.{} {}<=>{}".format(table_name, column_name, column['type'], data_column_type))

    def is_satisfied_by_is_numeric_policy(self, data_table, table_name, column):

        if column['column_name'] not in data_table.columns:
            return

        if column['type'] not in NUMERIC_TYPES:
            return

        series = data_table[column['column_name']]
        series = series[~series.isna()]
        ok_mask = series.apply(np.isreal)
        if not ok_mask.all():
            error_values = " ".join(list(set(series[~ok_mask])))[:200]
            self.errors.append("CRITICAL ERROR Column {}.{} has non-numeric values: {}".format(table_name, column['column_name'], error_values))

    def is_satisfied_by_has_pk_policy(self, submission, table_name, data_table):

        pk_name = submission.MetaData.get_pk_name(table_name)

        if pk_name not in data_table.columns:
            self.errors.append('CRITICAL ERROR Table {} has no PK named "{}"'.format(table_name, pk_name))

    def is_satisfied_by_system_id_policy(self, submission, table_name, data_table): # pylint: disable=unused-argument
        # Must have a system identity
        # if not submission.has_system_id(table_name):
        if 'system_id' not in data_table.columns:
            self.errors.append("{0} has no system id data column".format(table_name))
            return

        if data_table.system_id.isnull().values.any():
            self.errors.append("CRITICAL ERROR {0} has missing system id values".format(table_name))

        try:
	        #duplicate_mask = data_table[~data_table.system_id.isna()].duplicated('system_id')
            duplicate_mask = data_table.duplicated('system_id')
            duplicates = [ int(x) for x in set(data_table[duplicate_mask].system_id) ]
            if len(duplicates) > 0:
	            error_values = " ".join([ str(x) for x in duplicates])[:200]
	            self.errors.append("CRITICAL ERROR Table {} has DUPLICATE system ids: {}".format(table_name, error_values))
        except Exception as ex:
            self.warnings.append('WARNING! Duplicate check of {}.{} failed'.format(table_name, "system_id"))

    def is_satisfied_by_id_is_fk_convention(self, submission, table_name, column):

        column_name = column['column_name']

        is_fk = submission.MetaData.is_fk(table_name, column_name)
        is_pk = submission.MetaData.is_pk(table_name, column_name)

        if column_name[-3:] == '_id' and not (is_fk or is_pk):
            self.warnings.append('WARNING! Column {}.{}: ends with "_id" but NOT marked as PK/FK'.format(table_name, column_name))

    def is_satisfied_by_no_missing_columns_policy(self, submission, table_name, data_table): # pylint: disable=unused-argument
        """ All fields in MetaData.Table.Fields MUST exist in DataTable.columns
        """
        meta_column_names = sorted(submission.MetaData.table_fields(table_name)['column_name'].values.tolist())
        data_column_names = sorted(submission.DataTables[table_name].columns.values.tolist()) \
            if submission.exists(table_name) and submission.MetaData.is_table(table_name) else []

        missing_column_names = list(set(meta_column_names) - set(data_column_names) - set(self.ignore_columns))
        extra_column_names = list(set(data_column_names) - set(meta_column_names) - set(self.ignore_columns) - set(['system_id']))

        if len(missing_column_names) > 0:
            self.errors.append("ERROR {0} has MISSING DATA columns: ".format(table_name) + (", ".join(missing_column_names)))

        if len(extra_column_names) > 0:
            self.warnings.append("WARNING {0} has EXTRA DATA columns: ".format(table_name) + (", ".join(extra_column_names)))

    def is_satisfied_by_lookup_data_policy(self, submission, table_name):

        if not submission.exists(table_name):
            return

        if not submission.MetaData.is_lookup_table(table_name):
            return

        data_table = submission.DataTables[table_name]
        pk_name = submission.MetaData.get_pk_name(table_name)

        if data_table[pk_name].isnull().any():
            self.errors.append("CRITICAL ERROR {} new values not allowed for lookup table.".format(table_name))

