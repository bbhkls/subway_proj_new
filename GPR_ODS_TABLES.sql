-- ODS
-- CREATING TABLES
create table ods_profile_card_post (
	execution_date timestamp,
	oid oid,
	id integer, 
	card_num bigint, 
	FIO varchar, 
	birthday date, 
	phone_num varchar, 
	service_name varchar, 
	discount integer
);

create table ods_receipt_post (
	execution_date timestamp,
	oid oid,
	id_operation integer, 
	id_seller integer, 
	id_buyer integer, 
	id_product integer, 
	id_product_connection integer, 
	id_disc_card bigint,
	sel_dttm timestamp, 
	cnt integer, 
	price float(2), 
	combo_group integer, 
	tovar_group integer
);

-- META
CREATE TABLE dbt_schema.metadata_airflow_test ( -- rename on metadata in future
	run_id text,
	execution_date timestamp,
	source_n oid
);

-- DROP
drop table dbt_schema.ods_profile_card_post;
drop table dbt_schema.ods_profile_card_post_cut;
drop table dbt_schema.ods_receipt_post;
drop table dbt_schema.ods_receipt_post_cut;
drop table dbt_schema.metadata_airflow_test;

-- SELECT
-- SOURCE
select * from serps.profile_card_post;

-- ODS
select * from dbt_schema.ods_profile_card_post;
select * from dbt_schema.ods_profile_post_cut;

select * from dbt_schema.ods_receipt_post;
select * from dbt_schema.ods_receipt_post_cut;

select * from dbt_schema.ods_client_csv;
select * from dbt_schema.ods_client_cut ;

-- META
select * from dbt_schema.metadata_airflow_test;

-- DELETE
-- SOURCE
delete from serps.profile_card_post;

-- ODS
delete from dbt_schema.ods_profile_card_post;
delete from dbt_schema.ods_profile_post_cut;

delete from dbt_schema.ods_receipt_post;
delete from dbt_schema.ods_receipt_post_cut;

delete from dbt_schema.ods_client_csv;
delete from dbt_schema.ods_client_cut;

-- META
delete from dbt_schema.metadata_airflow_test;