select 	table_name,
        column_name,
        sead_utility.underscore_to_pascal_case(column_name, TRUE) as xml_column_name,
        ordinal_position as position,
        data_type,
        coalesce(numeric_precision, 0) as numeric_precision,
        coalesce(numeric_scale, 0) as numeric_scale,
        coalesce(character_maximum_length, 0) as character_maximum_length,
        case when is_nullable = 'YES' then true else false end as is_nullable,
        case when is_pk = 'YES' then true else false end as is_pk,
        case when is_fk = 'YES' then true else false end as is_fk,
        coalesce(t.fk_table_name, '') as fk_table_name,
        coalesce(t.fk_column_name, '') as fk_column_name,
        case
            when is_fk = 'YES' then sead_utility.underscore_to_pascal_case(t.fk_table_name)
            when data_type = 'integer' then 'java.lang.Integer'
            when data_type = 'smallint' then 'java.lang.Short'
            when data_type = 'boolean' then	'java.lang.Boolean'
            when data_type = 'character varying' then 'java.lang.String'
            when data_type = 'text' then 'java.lang.String'
            when data_type like 'timestamp%' then 'java.util.Date'
            when data_type = 'date' then 'java.util.Date'
            when data_type = 'numeric' then 'java.math.BigDecimal'
            else '???' end as class_name
from sead_utility.table_columns t
where t.table_schema = 'public'
    and t.table_name like 'tbl_%'
