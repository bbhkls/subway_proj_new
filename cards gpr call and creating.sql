-- New sources GPR tables
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

-- TRANSACTION LINK FOR RECEIPT
CREATE TABLE dbt_schema."GPR_RV_T_RECEIPT_POST" (
	dataflow_id varchar,
	dataflow_dttm timestamp,
	receip_rk varchar,
	shop_rk varchar,
	client_rk varchar,
	plu_rk varchar,
	plu_x_plu_rk varchar,
	card_rk varchar,
	sel_dttm timestamp, 
	cnt integer, 
	price float(2), 
	combo_group integer, 
	tovar_group integer
);

-- CARD DIM
-- RV
CREATE TABLE dbt_schema."GPR_RV_H_CARD" (
	dataflow_id varchar,
	dataflow_dttm timestamp,
	source_system_dk oid,
	card_rk varchar,
	hub_key text
);

CREATE TABLE dbt_schema."GPR_RV_S_PROFILE_CARD_POST" (
	dataflow_id text,
	dataflow_dttm text,
	source_system_dk oid,
	card_rk text,
	valid_from_dttm timestamp,
	hashdiff_key text,
	actual_flg integer,
	delete_flg integer,
	card_num_cnt bigint,
	card_service_name_desc varchar,
	discount_procent_cnt integer
);

CREATE TABLE dbt_schema."GPR_RV_E_CARD" (
	dataflow_id varchar,
	dataflow_dttm timestamp,
	hashdiff_key text,
	card_rk text,
	delete_flg integer,
	actual_flg integer,
	source_system_dk oid,
	valid_from_dttm timestamptz
);

-- BV
CREATE TABLE dbt_schema."GPR_BV_A_CARD" (
	dataflow_id varchar,
	dataflow_dttm timestamp,
	card_rk text,
	x_card_rk text
);

CREATE TABLE dbt_schema."GPR_BV_P_CARD" (
	dataflow_id text,
	dataflow_dttm timestamp,
	card_rk text,
	valid_from_dttm timestamp,
	valid_to_dttm timestamp,
	profile_card_post_vf_dttm timestamp
);

-- EM
CREATE TABLE dbt_schema."GPR_EM_DIM_CARD" (
	dataflow_id text,
	dataflow_dttm timestamp,
	client_rk text,
	valid_from_dttm timestamp,
	valid_to_dttm timestamp,
	card_num_cnt bigint,
	card_service_name_desc varchar,
	discount_procent_cnt integer
);

-- DROP
drop table dbt_schema.ods_profile_card_post;;
drop table dbt_schema.ods_profile_card_post_cut;;
drop table dbt_schema.ods_receipt_post;
drop table dbt_schema.ods_receipt_post_cut;
drop table dbt_schema."GPR_RV_H_CARD";
drop table dbt_schema."GPR_RV_S_PROFILE_CARD_POST";
drop table dbt_schema."GPR_RV_E_CARD";
drop table dbt_schema."GPR_RV_T_RECEIPT_POST";
drop table dbt_schema."GPR_BV_A_CARD";
drop table dbt_schema."GPR_BV_P_CARD";
drop table dbt_schema."GPR_EM_DIM_CARD";

-- SELECT
-- SOURCE
select * from serps.profile_card_post;
select * from serps.receipt_post;

-- ODS
select * from dbt_schema.ods_profile_card_post;
select * from dbt_schema.ods_profile_card_post_cut;

select * from dbt_schema.ods_receipt_post;
select * from dbt_schema.ods_receipt_post_cut;

-- RV
select * from dbt_schema."GPR_RV_H_CARD";
select * from dbt_schema."GPR_RV_S_PROFILE_CARD_POST";
select * from dbt_schema."GPR_RV_E_CARD";
select * from dbt_schema."GPR_RV_T_RECEIPT_POST";

-- BV
select * from dbt_schema."GPR_BV_A_CARD";
select * from dbt_schema."GPR_BV_P_CARD";

-- EM
select * from dbt_schema."GPR_EM_DIM_CARD";

-- META
select * from dbt_schema.metadata_airflow_test;

-- DELETE
-- SOURCE
delete from serps.profile_card_post;
delete from serps.receipt_post;

-- ODS
delete from dbt_schema.ods_profile_card_post;
delete from dbt_schema.ods_profile_card_post_cut;

delete from dbt_schema.ods_receipt_post;
delete from dbt_schema.ods_receipt_post_cut;

-- RV
delete from dbt_schema."GPR_RV_H_CARD";
delete from dbt_schema."GPR_RV_S_PROFILE_CARD_POST";
delete from dbt_schema."GPR_RV_E_CARD";
delete from dbt_schema."GPR_RV_T_RECEIPT_POST";

-- BV
delete from dbt_schema."GPR_BV_A_CARD";
delete from dbt_schema."GPR_BV_P_CARD";

-- EM
delete from dbt_schema."GPR_EM_DIM_CARD";

-- META
delete from dbt_schema.metadata_airflow_test;