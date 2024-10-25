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
