SELECT submission_id, submission_state_id, data_types, upload_user_id, upload_date, upload_content, status_text, claim_user_id, claim_date_time
FROM clearing_house.tbl_clearinghouse_submissions
WHERE submission_id = 79
;
with content_statistics as (
	select 'content_tables' as "table", submission_id, count(*)
	from clearing_house.tbl_clearinghouse_submission_xml_content_tables
	group by submission_id
	union
	select 'content_columns' as "table", submission_id, count(*)
	from clearing_house.tbl_clearinghouse_submission_xml_content_columns
	group by submission_id
	union
	select 'content_records' as "table", submission_id, count(*)
	from clearing_house.tbl_clearinghouse_submission_xml_content_records
	group by submission_id
	union
	select 'content_values' as "table", submission_id, count(*)
	from clearing_house.tbl_clearinghouse_submission_xml_content_values
	group by submission_id
) select *
  from content_statistics
  --join clearing_house.tbl_clearinghouse_submissions using (submission_id)
order by content_statistics
;
select distinct submission_id, count (distinct submission_id)
from clearing_house.tbl_clearinghouse_submission_xml_content_columns
;
select distinct submission_id, count (distinct submission_id)
from clearing_house.tbl_clearinghouse_submission_xml_content_values
;
/*
delete from clearing_house.tbl_clearinghouse_submission_xml_content_values;
delete from clearing_house.tbl_clearinghouse_submission_xml_content_records;
delete from clearing_house.tbl_clearinghouse_submission_xml_content_columns;
delete from clearing_house.tbl_clearinghouse_submission_xml_content_tables;
delete from clearing_house.tbl_clearinghouse_submissions;
*/

-- select sead_utility.sync_sequences('clearing_house')


with prev_content as (
	select *
	from clearing_house.tbl_clearinghouse_submission_xml_content_tables
	where submission_id = 80
), next_content as (
	select *
	from clearing_house.tbl_clearinghouse_submission_xml_content_tables
	where submission_id = 81
)
select *
from prev_content
full outer join next_content using (table_id)
where TRUE and (
	next_content.record_count is distinct from prev_content.record_count
)
;

with prev_content as (
	select *
	from clearing_house.tbl_clearinghouse_submission_xml_content_columns
	where submission_id = 80
), next_content	as (
	select *
	from clearing_house.tbl_clearinghouse_submission_xml_content_columns
	where submission_id = 81
)
select *
from prev_content
full outer join next_content using (table_id, column_name)
where TRUE and (
     next_content.column_name_underscored is distinct from prev_content.column_name_underscored
  or next_content.data_type is distinct from prev_content.data_type
  or next_content.fk_flag is distinct from prev_content.fk_flag
  or next_content.fk_table is distinct from prev_content.fk_table
  or next_content.fk_table_underscored is distinct from prev_content.fk_table_underscored
)
;


with prev_content as (
	select *
	from clearing_house.tbl_clearinghouse_submission_xml_content_records
	where submission_id = 80
	  and public_db_id is not null
), next_content	as (
	select *
	from clearing_house.tbl_clearinghouse_submission_xml_content_records
	where submission_id = 81
	  and public_db_id is not null
)
select *
from prev_content
full outer join next_content using (table_id, public_db_id)
where TRUE
  and next_content.public_db_id is distinct from prev_content.public_db_id
limit 100
;

select t.table_name, r.local_db_id, r.public_db_id
from clearing_house.tbl_clearinghouse_submission_xml_content_records r
join clearing_house.tbl_clearinghouse_submission_tables t using (table_id)
where r.submission_id = 81
order by 1
;
select * from clearing_house.tbl_abundances where local_db_id = 18197
/*
-- select clearing_house.fn_extract_and_store_submission_tables(2);
select clearing_house.fn_extract_and_store_submission_columns(2)
select clearing_house.fn_extract_and_store_submission_records(2)
select clearing_house.fn_extract_and_store_submission_values(2)
*/

select *
from clearing_house.fn_select_xml_content_records(81)


With submission_xml_data_rows As (
	Select x.submission_id,
		   unnest(xpath('/sead-data-upload/*/*', x.xml)) As xml
	From clearing_house.tbl_clearinghouse_submission_xml x
	Where Not xml Is Null
	  And xml Is Document
	  And x.submission_id = 81
)
	Select v.submission_id,
		   v.table_name::character varying(255),
		   Case When v.local_db_id ~ '^[0-9\.]+$' Then v.local_db_id::numeric::int Else Null End,
		   Case When v.public_db_id_attribute ~ '^[0-9\.]+$' Then v.public_db_id_attribute::numeric::int Else Null End,
		   Case When v.public_db_id_value ~ '^[0-9\.]+$' Then v.public_db_id_value::numeric::int Else Null End,
		   xml
	From (
		Select	d.submission_id																			as submission_id,
				replace(substring(d.xml::text from '^<([[:alnum:]\.]+).*>'), 'com.sead.database.', '')	as table_name,
				((xpath('//@id', d.xml))[1])::character varying(255)									as local_db_id,
				((xpath('//@clonedId', d.xml))[1])::character varying(255)							as public_db_id_attribute,
				((xpath('//clonedId/text()', d.xml))[1])::character varying(255)						as public_db_id_value,
				d.xml
		From submission_xml_data_rows as d
	) As v
	where v.table_name::character varying(255) = 'TblAbundances'
	;

with xml_test as (
	select '<com.sead.database.TblAbundances id="3930">
		  <abundanceId class="java.lang.Integer">3930</abundanceId>
		  <taxonId class="com.sead.database.TblTaxaTreeMaster" id="18197" clonedId="18197"/>
		  <analysisEntityId class="com.sead.database.TblAnalysisEntities" id="4191"/>
		  <abundanceElementId class="com.sead.database.TblAbundanceElements" id="44" clonedId="44"/>
		  <abundance class="java.lang.Integer">1.0</abundance>
		  <clonedId class="java.util.Integer">NULL</clonedId>
		  <dateUpdated class="java.util.Date"/>
		</com.sead.database.TblAbundances>
		'::xml as xml
) select
	replace(substring(d.xml::text from '^<([[:alnum:]\.]+).*>'), 'com.sead.database.', '')	as table_name,
	((xpath('/*/@id', d.xml))[1])::character varying(255)									as local_db_id,
	((xpath('/*/@clonedId', d.xml))[1])::character varying(255)							as public_db_id_attribute,
	((xpath('/*/clonedId/text()', d.xml))[1])::character varying(255)						as public_db_id_value
  from xml_test as d


  		select
                  d.table_name																as table_name,
                substring(d.xml::text from '^<([[:alnum:]]+).*>')::character varying(255)	as column_name,
                (xpath('/*/@class', d.xml))[1]::character varying(255)					as column_type
        From (
            Select x.submission_id, t.table_name, unnest(xpath('/sead-data-upload/' || t.table_name || '/*[not(@clonedId)][1]/*', xml)) As xml
            From clearing_house.tbl_clearinghouse_submissions x
            Join clearing_house.fn_select_xml_content_tables(81) t
              On t.submission_id = x.submission_id
            Where 1 = 1
              And x.submission_id = 81
              And Not xml Is Null
              And xml Is Document
        ) as d;


With record_xml As (
	Select x.submission_id, unnest(xpath('/sead-data-upload/*/*', x.xml))			As xml
	From clearing_house.tbl_clearinghouse_submissions x
	Where x.submission_id = 81
	  And Not x.xml Is Null
	  And x.xml Is Document
), record_value_xml As (
	Select	x.submission_id																				As submission_id,
			replace(substring(x.xml::text from '^<([[:alnum:]\.]+).*>'), 'com.sead.database.', '')		As table_name,
			nullif((xpath('/*/@id', x.xml))[1]::character varying(255), 'NULL')::numeric::int			As local_db_id,
			nullif((xpath('/*/@clonedId', x.xml))[1]::character varying(255), 'NULL')::numeric::int	    As public_db_id,
			unnest(xpath( '/*/*', x.xml))																As xml
	From record_xml x
)   Select	x.submission_id																				As submission_id,
			x.table_name::character varying																As table_name,
			x.local_db_id																				As local_db_id,
			x.public_db_id																				As public_db_id,
			substring(x.xml::character varying(255) from '^<([[:alnum:]]+).*>')::character varying(255)	As column_name,
			nullif((xpath('/*/@class', x.xml))[1]::character varying, 'NULL')::character varying		    As column_type,
			nullif((xpath('/*/@id', x.xml))[1]::character varying(255), 'NULL')::numeric::int			As fk_local_db_id,
			nullif((xpath('/*/@clonedId', x.xml))[1]::character varying(255), 'NULL')::numeric::int	    As fk_public_db_id,
			nullif((xpath('/*/text()', x.xml))[1]::text, 'NULL')::text									As value
	From record_value_xml x;


select count(*)
from clearing_house.tbl_clearinghouse_submissions
/*
with prev_content_tables as (
	select *
	from clearing_house.tbl_clearinghouse_submission_xml_content_tables
	where submission_id = 1
), next_content_tables as (
	select *
	from clearing_house.tbl_clearinghouse_submission_xml_content_tables
	where submission_id = 999
)
select *
from prev_content_tables
full outer join next_content_tables using (table_id)
left join clearing_house.tbl_clearinghouse_submission_tables using (table_id)
where next_content_tables.table_id is null
   or prev_content_tables.table_id is null


	| tbl_clearinghouse_submission_xml_content_columns    | Unique column names and types in uploaded data |
	| tbl_clearinghouse_submission_xml_content_records    | Unique records (rows) in uploaded data         |
	| tbl_clearinghouse_submission_xml_content_values	  | Values in uploaded data                        |
*/



select format('LEFT JOIN (select %2$s, analysis_entity_id from %1$s) USING (analysis_entity_id) as %1$s', FK.table_name, PK.column_name)
--select format('select ''%1$s'' as table_name, %2$s as entity_id from %1$s', table_name, column_name)
from sead_utility.table_columns FK
inner join sead_utility.table_columns PK
  on PK.table_name = FK.table_name
 and PK.is_pk = 'YES'
where 1 = 1
  and FK.is_pk = 'NO'
  and FK.column_name = 'analysis_entity_id'



select master_name as category,
	count(distinct site_id) as site_count,
	count(distinct location_id) as location_count
	count(distinct dataset_id) as dataset_count,
	count(distinct sample_group_id) as sample_group_count,
	count(distinct physical_sample_id) as sample_count,
	count(distinct analysis_entity_id) as analysis_count,
	count(distinct abundance_id) as abundunce_row_count,
	count(distinct measured_value_id) as value_count,
	count(distinct isotope_id) as isotope_count,
	count(distinct ceramics_id) as ceramic_count,
	count(distinct dendro_id) as dendro_count,
from tbl_dataset_masters
join tbl_datasets using (master_set_id)
left join tbl_analysis_entities using (dataset_id)
left join tbl_physical_samples using (physical_sample_id)
left join tbl_sample_groups using (sample_group_id)
left join tbl_sites using (site_id)
left join tbl_site_locations using (site_id)
LEFT JOIN (select abundance_id, analysis_entity_id from tbl_abundances) USING (analysis_entity_id) as tbl_abundances
LEFT JOIN (select measured_value_id, analysis_entity_id from tbl_measured_values) USING (analysis_entity_id) as tbl_measured_values
LEFT JOIN (select isotope_id, analysis_entity_id from tbl_isotopes) USING (analysis_entity_id) as tbl_isotopes
LEFT JOIN (select ceramics_id, analysis_entity_id from tbl_ceramics) USING (analysis_entity_id) as tbl_ceramics
LEFT JOIN (select dendro_id, analysis_entity_id from tbl_dendro) USING (analysis_entity_id) as tbl_dendro
-- LEFT JOIN (select aggregate_sample_id, analysis_entity_id from tbl_aggregate_samples) USING (analysis_entity_id) as tbl_aggregate_samples
-- LEFT JOIN (select analysis_entity_age_id, analysis_entity_id from tbl_analysis_entity_ages) USING (analysis_entity_id) as tbl_analysis_entity_ages
-- LEFT JOIN (select analysis_entity_dimension_id, analysis_entity_id from tbl_analysis_entity_dimensions) USING (analysis_entity_id) as tbl_analysis_entity_dimensions
-- LEFT JOIN (select analysis_entity_prep_method_id, analysis_entity_id from tbl_analysis_entity_prep_methods) USING (analysis_entity_id) as tbl_analysis_entity_prep_methods
-- LEFT JOIN (select dendro_date_id, analysis_entity_id from tbl_dendro_dates) USING (analysis_entity_id) as tbl_dendro_dates
-- LEFT JOIN (select geochron_id, analysis_entity_id from tbl_geochronology) USING (analysis_entity_id) as tbl_geochronology
-- LEFT JOIN (select relative_date_id, analysis_entity_id from tbl_relative_dates) USING (analysis_entity_id) as tbl_relative_dates
-- LEFT JOIN (select tephra_date_id, analysis_entity_id from tbl_tephra_dates) USING (analysis_entity_id) as tbl_tephra_dates
where 1 = 1
group by master_name



select n.nspname as schema_name, c.relname as table_name, col.attname as column_name, d.description as comment
from pg_class c
join pg_namespace n on c.relnamespace = n.oid
left join pg_description d on c.oid = d.objoid
left join pg_attribute col on c.oid = col.attrelid and col.attnum = d.objsubid
where c.relkind = 'r' -- only tables
  or (c.relkind = 'v' and col.attnum is null) -- views without specific column comments
order by n.nspname, c.relname, col.attnum
select site_name
from sead_utility.table_columns


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

