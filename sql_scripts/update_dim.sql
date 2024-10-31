-- обновление концов временных интервалов
update dbt_schema."GPR_EM_DIM_{{params.dim}}" dm
set (dataflow_id, dataflow_dttm, valid_to_dttm) = (select '{{run_id}}', '{{execution_date}}'::timestamp, valid_to_dttm 
													 from dbt_schema."GPR_BV_P_{{params.dim}}"
													where dm.client_rk = client_rk and dm.valid_from_dttm = valid_from_dttm)
where valid_to_dttm <> (select valid_to_dttm from dbt_schema."GPR_BV_P_{{params.dim}}" where dm.client_rk = client_rk and dm.valid_from_dttm = valid_from_dttm);

commit;
