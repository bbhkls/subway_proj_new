-- TRANSACTIONS

-- CREATING
-- TRANSACTION LINK FOR RECEIPT
CREATE TABLE dbt_schema."GPR_RV_T_RECEIPT_POST" ( -- rename post -> ora in future
	dataflow_id varchar,
	dataflow_dttm timestamp,
	receip_rk varchar,
	shop_rk varchar,
	client_rk varchar,
	plu_rk varchar,
	plu_x_plu_rk varchar,
	card_cnt bigint,
	receip_num_cnt integer,
	sel_dttm timestamp, 
	cnt integer, 
	price float(2), 
	combo_group integer, 
	tovar_group integer
);

-- DROP
drop table dbt_schema."GPR_RV_T_RECEIPT_POST";

-- SELECT
-- RV
select * from dbt_schema."GPR_RV_T_RECEIPT_POST";

-- META
select * from dbt_schema.metadata_airflow_test;

-- DELETE
-- RV
delete from dbt_schema."GPR_RV_T_RECEIPT_POST";

-- META
delete from dbt_schema.metadata_airflow_test;