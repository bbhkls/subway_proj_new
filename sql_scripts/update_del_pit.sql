update dbt_schema."GPR_BV_P_CLIENT"								
set (dataflow_id, dataflow_dttm, valid_to_dttm) = (select run_id, execution_date, execution_date from dbt_schema.metadata_airflow where source_n = 'csv')
where ((client_rk, valid_from_dttm, 1) in (select ac.client_rk a, max(valid_from_dttm), min(delete_flg)
										from dbt_schema."GPR_RV_S_CLIENT" sc 
										join dbt_schema."GPR_BV_A_CLIENT" ac on sc.client_rk = ac.x_client_rk 
										group by ac.client_rk)
   or client_rk not in (select client_rk from dbt_schema."GPR_BV_A_CLIENT")) and extract(year from valid_to_dttm) = 5999;

commit;