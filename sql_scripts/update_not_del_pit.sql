-- обновление конца периода в случае, когда экземпляр сущности не удален
with ad_tab as (
	select dataflow_id, dataflow_dttm, ld, client_rk, valid_from_dttm
	from (
	select client_rk, '{{run_id}}' dataflow_id, '{{execution_date}}'::timestamp dataflow_dttm, 
	       valid_to_dttm, valid_from_dttm,
		   lead(valid_from_dttm, 1, valid_to_dttm) over (partition by client_rk, valid_to_dttm order by valid_from_dttm) ld
	from dbt_schema."GPR_BV_P_{{params.dim}}")
	where valid_to_dttm <> ld
)

update dbt_schema."GPR_BV_P_{{params.dim}}" pc
set (dataflow_id, dataflow_dttm, valid_to_dttm) = (select dataflow_id, dataflow_dttm, ld - interval '1 minute'
					   							   from ad_tab
							    				   where pc.client_rk = client_rk)
where (client_rk, valid_from_dttm) in (select client_rk, valid_from_dttm
										from ad_tab);

commit; 