with sead_columns as (
	select 	table_name,
		   	column_name,
		   	clearing_house.fn_underscore_to_pascal(column_name, TRUE) as xml_version,
			ordinal_position as position,
		   	data_type as "type",
			numeric_precision,
			numeric_scale,
			character_maximum_length as length,
			is_nullable,
			is_pk,
			is_fk,
			fk_table_name,
			fk_column_name,
			case
				when is_fk = 'YES' then sead_utility.underscore_to_pascal_case(fk_table_name)
				when data_type = 'integer' then 'java.lang.Integer'
				when data_type = 'bigint' then 'java.lang.Integer'
				when data_type = 'smallint' then 'java.lang.Short'
				when data_type = 'date' then 'java.sql.Date'
				when data_type = 'boolean' then	'java.lang.Boolean'
				when data_type = 'character varying' then 'java.lang.String'
				when data_type = 'text' then 'java.lang.String'
				when data_type like 'timestamp%' then 'java.util.Date'
				when data_type = 'date' then 'java.sql.Date'
				when data_type = 'numeric' then 'java.math.BigDecimal'
				else 'error['  || data_type || ']' end as class_name
	from sead_utility.table_columns
	where table_schema = 'public'
	  and table_name like 'tbl_%'
) select table_name, column_name,
	case when s.xml_version = xl.xml_version then 'OK' else 'DIFF' end as xml_name_check,
	case when s.position = xl.position then 'OK' else 'DIFF' end as position_check,
	case when s.type = xl.type then 'OK' else 'DIFF' end as type_check,
	case when coalesce(s.length, 0) = coalesce(xl.length, 0) then 'OK' else 'DIFF' end as length_check,
	case when coalesce(s.class_name, '') = coalesce(xl.class, '') then 'OK' else 'DIFF' end as class_check,
	xl.class, s.class_name, s.type 
  from sead_columns s
  join import_metadata_columns xl using (table_name, column_name)
  where TRUE
    -- and s.is_fk = 'NO'
  	and coalesce(s.class_name, '') <> coalesce(xl.class, '')
	
	and table_name not in ('tbl_updates_log')
	and not (xl.class = 'java.long.Short' and s.class_name = 'java.lang.Short')
	and not (lower(xl.class) = lower(s.class_name))

-- | "table_name"                   | "column_name"               | "class_check" | "class"                   | "class_name"          | "type"            |
-- | :----------------------------- | :-------------------------- | :------------ | :------------------------ | :-------------------- | :---------------- |
-- | tbl_aggregate_sample_ages      | analysis_entity_age_id      | DIFF          | TblAnalysisEntities       | TblAnalysisEntityAges | integer           | Wrong type in excel
-- | tbl_analysis_entity_dimensions | dimension_value             | DIFF          | java.lang.Long            | java.math.BigDecimal  | numeric           | Wrong type in excel
-- | tbl_biblio                     | year                        | DIFF          | java.lang.Integer         | java.lang.String      | character varying | Wrong type in excel
-- | tbl_chronologies               | relative_age_type_id        | DIFF          | TblRelativeAgeTypes       | java.lang.Integer     | integer           | Missing FK?
-- | tbl_contacts                   | location_id                 | DIFF          | TblLocations              | java.lang.Integer     | integer           | Missing FK?
-- | tbl_dataset_submissions        | date_submitted              | DIFF          | java.lang.String          | java.sql.Date         | date              | Wrong type in SEAD 
-- | tbl_mcr_names                  | taxon_id                    | DIFF          | java.lang.Integer         | TblTaxaTreeMaster     | integer           | Wrong type in excel
-- | tbl_sites                      | site_preservation_status_id | DIFF          | TblSitePreservationStatus | java.lang.Integer     | integer           | Missing FK? site pekar på status & status pekar på site!
-- | tbl_isotopes                   | measurement_value           | DIFF          | java.math.BigDecimal      | java.lang.String      | text              | Wrong type in excel?

