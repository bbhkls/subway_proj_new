-- CLIENT DIM

-- CREATING
-- RV
CREATE TABLE dbt_schema."GPR_RV_H_CLIENT" (
	dataflow_id varchar,
	dataflow_dttm timestamp,
	source_system_dk oid,
	client_rk varchar,
	hub_key text
);

CREATE TABLE dbt_schema."GPR_RV_M_CLIENT_SUBWAY_STAR" (
	dataflow_id text,
	dataflow_dttm timestamp,
	source_system_dk oid,
	client_rk text,
	row_num integer,
	valid_from_dttm timestamp,
	hashdiff_key text,
	actual_flg integer,
	delete_flg integer,
	name_desc text,
	phone_desc varchar,
	city_desc text,
	birthday_dt date,
	age_cnt integer
);

CREATE TABLE dbt_schema."GPR_RV_M_CLIENT_PRFOLIE_POST" (
	dataflow_id text,
	dataflow_dttm timestamp,
	source_system_dk oid,
	client_rk text,
	row_num integer,
	valid_from_dttm timestamp,
	hashdiff_key text,
	actual_flg integer,
	delete_flg integer,
	FIO_desc text,
	phone_desc varchar,
	birthday_dt date
);

CREATE TABLE dbt_schema."GPR_RV_E_CLIENT" (
	dataflow_id varchar,
	dataflow_dttm timestamp,
	hashdiff_key text,
	client_rk text,
	delete_flg integer,
	actual_flg integer,
	source_system_dk oid,
	valid_from_dttm timestamptz
);

-- BV
CREATE TABLE dbt_schema."GPR_BV_A_CLIENT" (
	dataflow_id varchar,
	dataflow_dttm timestamp,
	client_rk text,
	x_client_rk text
);

CREATE TABLE dbt_schema."GPR_BV_P_CLIENT" (
	dataflow_id text,
	dataflow_dttm timestamp,
	client_rk text,
	valid_from_dttm timestamp,
	valid_to_dttm timestamp,
	client_subway_star_vf_dttm timestamp,
	profile_client_post_vf_dttm timestamp
);

-- EM
CREATE TABLE dbt_schema."GPR_EM_DIM_CLIENT" (
	dataflow_id text,
	dataflow_dttm timestamp,
	client_rk text,
	valid_from_dttm timestamp,
	valid_to_dttm timestamp,
	client_name_desc text,
	client_phone_desc text,
	client_city_desc text,
	client_birthday_dt date,
	client_age_cnt integer
);

-- DROP
drop table dbt_schema."GPR_RV_H_CLIENT";
drop table dbt_schema."GPR_RV_M_CLIENT_SUBWAY_STAR";
drop table dbt_schema."GPR_RV_M_CLIENT_PRFOLIE_POST";
drop table dbt_schema."GPR_RV_E_CLIENT";
drop table dbt_schema."GPR_BV_A_CLIENT";
drop table dbt_schema."GPR_BV_P_CLIENT";
drop table dbt_schema."GPR_EM_DIM_CLIENT";

-- SELECT
-- RV
select * from dbt_schema."GPR_RV_H_CLIENT";
select * from dbt_schema."GPR_RV_M_CLIENT_SUBWAY_STAR";
select * from dbt_schema."GPR_RV_M_CLIENT_PRFOLIE_POST";
select * from dbt_schema."GPR_RV_E_CLIENT";

-- BV
select * from dbt_schema."GPR_BV_A_CLIENT";
select * from dbt_schema."GPR_BV_P_CLIENT";

-- EM
select * from dbt_schema."GPR_EM_DIM_CLIENT";

-- META
select * from dbt_schema.metadata_airflow_test;

-- DELETE
-- RV
delete from dbt_schema."GPR_RV_H_CLIENT";
delete from dbt_schema."GPR_RV_M_CLIENT_SUBWAY_STAR";
delete from dbt_schema."GPR_RV_M_CLIENT_PRFOLIE_POST";
delete from dbt_schema."GPR_RV_E_CLIENT";

-- BV
delete from dbt_schema."GPR_BV_A_CLIENT";
delete from dbt_schema."GPR_BV_P_CLIENT"

-- EM
delete from dbt_schema."GPR_EM_DIM_CLIENT";

-- META
delete from dbt_schema.metadata_airflow_test;