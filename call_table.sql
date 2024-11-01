-- CLIENT
--Source
select * from serps.client_from_star;
--ODS
select * from dbt_schema.ods_client_csv;
select * from dbt_schema.ods_client_cut ;
--RV
select * from dbt_schema."GPR_RV_H_CLIENT";
select * from dbt_schema."GPR_RV_S_CLIENT";
select * from dbt_schema."GPR_RV_E_CLIENT";
--BV
select * from dbt_schema."GPR_BV_A_CLIENT";
select * from dbt_schema."GPR_BV_P_CLIENT";
--EM
select * from dbt_schema."GPR_EM_DIM_CLIENT";
--META
select * from dbt_schema.metadata_airflow_test;

--Source
delete from serps.client_from_star;
--ODS
delete from dbt_schema.ods_client_csv;
delete from dbt_schema.ods_client_cut ;
--RV
delete from dbt_schema."GPR_RV_H_CLIENT";
delete from dbt_schema."GPR_RV_S_CLIENT";
delete from dbt_schema."GPR_RV_E_CLIENT";
--BV
delete from dbt_schema."GPR_BV_A_CLIENT";
delete from dbt_schema."GPR_BV_P_CLIENT";
--EM
delete from dbt_schema."GPR_EM_DIM_CLIENT";
--META
delete from dbt_schema.metadata_airflow_test;

-- CARD
-- SELECT
--ODS 
select * from dbt_schema.ods_card_csv;
select * from dbt_schema.ods_card_cut ;
--RV
select * from dbt_schema."GPR_RV_H_CARD";
select * from dbt_schema."GPR_RV_S_CARD";
select * from dbt_schema."GPR_RV_E_CARD";
select * from dbt_schema."GPR_RV_L_CARD_X_CLIENT";
--BV
select * from dbt_schema."GPR_BV_A_CARD_CLIENT";
select * from dbt_schema."GPR_BV_P_CARD";
--EM
select * from dbt_schema."GPR_EM_DIM_CARD";
--META
select * from dbt_schema.metadata_airflow_test;

-- DELETE
--ODS
delete from dbt_schema.ods_card_csv;
delete from dbt_schema.ods_card_cut ;
--RV
delete from dbt_schema."GPR_RV_H_CARD";
delete from dbt_schema."GPR_RV_S_CARD";
delete from dbt_schema."GPR_RV_E_CARD";
delete from dbt_schema."GPR_RV_L_CARD_X_CLIENT";
--BV
delete from dbt_schema."GPR_BV_A_CARD_CLIENT";
delete from dbt_schema."GPR_BV_P_CARD";
--EM
delete from dbt_schema."GPR_EM_DIM_CARD";
--META
delete from dbt_schema.metadata_airflow_test;