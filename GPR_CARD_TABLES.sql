-- CARD DIM

-- CREATING
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

CREATE TABLE dbt_schema."GPR_BV_B_RECEIPT_CARD_POST" (
	dataflow_id varchar,
	dataflow_dttm timestamp,
	card_tech_key varchar,
	valid_from_dttm timestamp,
	client_name_desc varchar,
	client_phone_desc varchar,
	client_city_desc varchar,
	client_city_dt date, -- rename on client_birthday_dt in future
	client_age_cnt integer
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
drop table dbt_schema."GPR_RV_H_CARD";
drop table dbt_schema."GPR_RV_S_PROFILE_CARD_POST";
drop table dbt_schema."GPR_RV_E_CARD";
drop table dbt_schema."GPR_BV_A_CARD";
drop table dbt_schema."GPR_BV_P_CARD";
drop table dbt_schema."GPR_BV_B_RECEIPT_CARD_POST";
drop table dbt_schema."GPR_EM_DIM_CARD";

-- SELECT
-- RV
select * from dbt_schema."GPR_RV_H_CARD";
select * from dbt_schema."GPR_RV_S_PROFILE_CARD_POST";
select * from dbt_schema."GPR_RV_E_CARD";

-- BV
select * from dbt_schema."GPR_BV_A_CARD";
select * from dbt_schema."GPR_BV_P_CARD";
select * from dbt_schema."GPR_BV_B_RECEIPT_CARD_POST";

-- EM
select * from dbt_schema."GPR_EM_DIM_CLIENT";

-- META
select * from dbt_schema.metadata_airflow_test;

-- DELETE
-- RV
delete from dbt_schema."GPR_RV_H_CARD";
delete from dbt_schema."GPR_RV_S_PROFILE_CARD_POST";
delete from dbt_schema."GPR_RV_E_CARD";

-- BV
delete from dbt_schema."GPR_BV_A_CARD";
delete from dbt_schema."GPR_BV_P_CARD";
delete from dbt_schema."GPR_BV_B_RECEIPT_CARD_POST";

-- EM
delete from dbt_schema."GPR_EM_DIM_CARD";

-- META
delete from dbt_schema.metadata_airflow_test;