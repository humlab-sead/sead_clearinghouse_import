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
	From clearing_house.tbl_clearinghouse_submissions x
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
	