-- обновление концов временных интервалов
update dbt_schema."GPR_EM_DIM_CLIENT" dm
set (dataflow_id, dataflow_dttm, valid_to_dttm) = (select ma.run_id, ma.execution_date, valid_to_dttm 
													 from dbt_schema."GPR_BV_P_CLIENT", (select * from dbt_schema.metadata_airflow_test where source_n = 'csv') ma 
													where dm.client_rk = client_rk and dm.valid_from_dttm = valid_from_dttm)
where valid_to_dttm <> (select valid_to_dttm from dbt_schema."GPR_BV_P_CLIENT" where dm.client_rk = client_rk and dm.valid_from_dttm = valid_from_dttm);

commit;
